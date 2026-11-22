//! Identifiers (user, click, campaign, product IDs) as 16-byte values.
//!
//! An ID of up to 15 bytes is stored inline: its bytes, zero padding, and its
//! length in the last byte, so equal values mean equal strings. A longer ID is
//! interned once and stored as a pointer with the marker length 0xFF, so equal
//! values still mean equal strings. The empty string is all zeros.

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
#[repr(C)]
pub struct Id {
    pub lo: u64,
    pub hi: u64,
}

const K1: u64 = 0x9E37_79B9_7F4A_7C15;
const K2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const K3: u64 = 0xA076_1D64_78BD_642F;
const K4: u64 = 0xE703_7ED1_A0B4_28DB;

static MASK: [(u64, u64); 16] = {
    let mut m = [(0u64, 0u64); 16];
    let mut n = 0;
    while n < 16 {
        let lo = if n >= 8 { u64::MAX } else { (1u64 << (8 * n)) - 1 };
        let hi = if n <= 8 { 0 } else { (1u64 << (8 * (n - 8))) - 1 };
        m[n] = (lo, hi);
        n += 1;
    }
    m
};

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let m = (a as u128).wrapping_mul(b as u128);
    (m as u64) ^ ((m >> 64) as u64)
}

/// The table of interned long IDs: open addressing over tagged pointers, filled by
/// compare-and-swap, sized once at start from the input. A slot holds 0 (empty),
/// CLOSED, or the address of an interned string (`[len: u64][bytes]`) with 16 bits of
/// the string's hash on top. A string's probe sequence ends at its entry or at the
/// first 0 or CLOSED slot. Past 70% full, a thread that reaches a 0 slot closes it
/// and interns the string in the sharded maps instead; a slot only ever changes from
/// 0 to an address or to CLOSED, so every thread finds a given string in one place.
struct LongTable {
    slots: &'static [AtomicU64],
    mask: usize,
    used: AtomicUsize,
    limit: usize,
}

const CLOSED: u64 = 1;
static LTAB: OnceLock<LongTable> = OnceLock::new();

/// Sizes the long-ID table for about `expected` distinct long IDs.
pub fn init_long_table(expected: usize, huge: bool) {
    let n = (expected.max(1 << 14) * 2).next_power_of_two();
    let bytes = n * 8;
    let p = crate::mem::map(bytes);
    if huge {
        crate::mem::hugepages(p, bytes);
    }
    let slots = unsafe { std::slice::from_raw_parts(p as *const AtomicU64, n) };
    let _ = LTAB.set(LongTable { slots, mask: n - 1, used: AtomicUsize::new(0), limit: n / 10 * 7 });
}

/// Returns part `k` of `n` of the long-ID table's pages to the kernel (once no ID is
/// interned or looked up any more: the interned strings themselves stay).
pub fn release_long_table_part(k: usize, n: usize) {
    if let Some(t) = LTAB.get() {
        let bytes = t.slots.len() * 8;
        let (lo, hi) = ((bytes / n * k) & !4095, if k + 1 == n { bytes } else { (bytes / n * (k + 1)) & !4095 });
        if hi > lo {
            crate::mem::release(unsafe { (t.slots.as_ptr() as *mut u8).add(lo) }, hi - lo);
        }
    }
}

thread_local! {
    /// This thread's space for interned strings: (next free byte, bytes left).
    static ARENA: Cell<(*mut u8, usize)> = const { Cell::new((std::ptr::null_mut(), 0)) };
}

/// A copy of `b` as `[len: u64][bytes]`, 8-byte aligned, that lives to process exit.
fn store(b: &[u8]) -> *const u8 {
    let need = (8 + b.len()).div_ceil(8) * 8;
    ARENA.with(|a| {
        let (mut p, mut left) = a.get();
        if left < need {
            let chunk = need.max(1 << 20);
            p = Box::leak(vec![0u64; chunk / 8].into_boxed_slice()).as_mut_ptr() as *mut u8;
            left = chunk;
        }
        unsafe {
            (p as *mut u64).write(b.len() as u64);
            std::ptr::copy_nonoverlapping(b.as_ptr(), p.add(8), b.len());
        }
        a.set((unsafe { p.add(need) }, left - need));
        p as *const u8
    })
}


/// Whether the interned string at `p` is `b`: lengths, then eight bytes at a time
/// (no call to a general compare; this runs once per long ID read).
#[inline(always)]
unsafe fn stored_eq(p: *const u8, b: &[u8]) -> bool {
    let n = b.len();
    if (p as *const u64).read() as usize != n {
        return false;
    }
    let (s, t) = (p.add(8), b.as_ptr());
    let mut i = 0;
    let mut diff = 0u64;
    while i + 8 <= n {
        diff |= (s.add(i) as *const u64).read_unaligned() ^ (t.add(i) as *const u64).read_unaligned();
        i += 8;
    }
    while i < n {
        diff |= (*s.add(i) ^ *t.add(i)) as u64;
        i += 1;
    }
    diff == 0
}

/// Looks `b` up in the long-ID table, adding it if absent; None when the table is
/// not set up or the string belongs to the sharded maps.
fn intern_table(b: &[u8], h: u64) -> Option<*const u8> {
    let t = LTAB.get()?;
    let tag = h & (0xFFFF << 48);
    let mut i = (h as usize) & t.mask;
    let mut mine: *const u8 = std::ptr::null();
    loop {
        let slot = &t.slots[i];
        let v = slot.load(Ordering::Acquire);
        if v == CLOSED {
            return None;
        }
        if v == 0 {
            let full = t.used.load(Ordering::Relaxed) >= t.limit;
            let new = if full {
                CLOSED
            } else {
                if mine.is_null() {
                    mine = store(b);
                }
                mine as u64 | tag
            };
            match slot.compare_exchange(0, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) if full => return None,
                Ok(_) => {
                    t.used.fetch_add(1, Ordering::Relaxed);
                    return Some(mine);
                }
                Err(_) => continue, // someone took this slot: look at what it holds
            }
        }
        if v & (0xFFFF << 48) == tag {
            let p = (v & ((1 << 48) - 1)) as *const u8;
            if unsafe { stored_eq(p, b) } {
                return Some(p);
            }
        }
        i = (i + 1) & t.mask;
    }
}

/// Hashes a long ID's bytes (the map below is keyed by them) eight bytes at a time.
#[derive(Default)]
struct LongHasher(u64);

impl std::hash::Hasher for LongHasher {
    fn write(&mut self, b: &[u8]) {
        let mut x = self.0 ^ K3 ^ b.len() as u64;
        for c in b.chunks(8) {
            let mut w = [0u8; 8];
            w[..c.len()].copy_from_slice(c);
            x = mum(x ^ u64::from_le_bytes(w), K1);
        }
        self.0 = x;
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

type LongMap = HashMap<&'static [u8], usize, std::hash::BuildHasherDefault<LongHasher>>;

/// Interned long IDs, in shards chosen by a hash of the bytes, so threads that
/// intern at the same time rarely wait on one another.
const SHARDS: usize = 64;
static LONG: [Mutex<Option<LongMap>>; SHARDS] = [const { Mutex::new(None) }; SHARDS];

/// The 128 bits spelled by the 32 lowercase hexadecimal digits at p (each digit's
/// value in turn, four bits each); None if any of the 32 bytes is not one.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
pub unsafe fn hex32(p: *const u8) -> Option<(u64, u64)> {
    use core::arch::x86_64::*;
    let v = _mm256_loadu_si256(p as *const __m256i);
    let dg = _mm256_sub_epi8(v, _mm256_set1_epi8(b'0' as i8)); // '0'..'9' -> 0..9
    let lt = _mm256_sub_epi8(v, _mm256_set1_epi8(b'a' as i8)); // 'a'..'f' -> 0..5
    let isd = _mm256_cmpeq_epi8(_mm256_min_epu8(dg, _mm256_set1_epi8(9)), dg);
    let isl = _mm256_cmpeq_epi8(_mm256_min_epu8(lt, _mm256_set1_epi8(5)), lt);
    if _mm256_movemask_epi8(_mm256_or_si256(isd, isl)) != -1 {
        return None;
    }
    let val = _mm256_blendv_epi8(_mm256_add_epi8(lt, _mm256_set1_epi8(10)), dg, isd);
    // Pairs of digits to bytes (first digit high), then the 16 bytes together.
    let pairs = _mm256_maddubs_epi16(val, _mm256_set1_epi16(0x0110));
    let packed = _mm256_permute4x64_epi64(_mm256_packus_epi16(pairs, pairs), 0b1000);
    Some((_mm256_extract_epi64::<0>(packed) as u64, _mm256_extract_epi64::<1>(packed) as u64))
}

impl Id {
    pub const EMPTY: Id = Id { lo: 0, hi: 0 };

    /// The inline ID for `len <= 15` bytes at `p`. Reads 16 bytes at `p`.
    #[inline(always)]
    pub unsafe fn short(p: *const u8, len: usize) -> Id {
        debug_assert!(len <= 15);
        let lo = (p as *const u64).read_unaligned();
        let hi = (p.add(8) as *const u64).read_unaligned();
        let (ml, mh) = *MASK.get_unchecked(len);
        Id { lo: lo & ml, hi: (hi & mh) | ((len as u64) << 56) }
    }

    /// The ID of any byte string; long ones are interned (rare, slow).
    #[cold]
    #[inline(never)]
    pub fn intern(b: &[u8]) -> Id {
        if b.len() <= 15 {
            let mut t = [0u8; 16];
            t[..b.len()].copy_from_slice(b);
            return unsafe { Id::short(t.as_ptr(), b.len()) };
        }
        let h = {
            use std::hash::Hasher;
            let mut h = LongHasher::default();
            h.write(b);
            h.finish()
        };
        Id::intern_long(b, h)
    }

    /// What `intern` computes first for a long ID: the hash of its `len` bytes at
    /// `p`, read eight at a time (reads up to 7 bytes past the end).
    #[inline(always)]
    pub unsafe fn long_hash(p: *const u8, len: usize) -> u64 {
        let mut x = K3 ^ len as u64;
        let mut i = 0;
        while i + 8 <= len {
            x = mum(x ^ (p.add(i) as *const u64).read_unaligned(), K1);
            i += 8;
        }
        if i < len {
            let w = (p.add(i) as *const u64).read_unaligned() & (u64::MAX >> (8 * (8 - (len - i))));
            x = mum(x ^ w, K1);
        }
        x
    }

    /// Starts loading the long-ID table slot a lookup with hash `h` reads first.
    #[inline(always)]
    pub fn prefetch_long(h: u64) {
        #[cfg(target_arch = "x86_64")]
        if let Some(t) = LTAB.get() {
            unsafe { core::arch::x86_64::_mm_prefetch(t.slots.as_ptr().add((h as usize) & t.mask) as *const i8, core::arch::x86_64::_MM_HINT_T0) }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = h;
    }

    /// Starts loading the interned string a lookup with hash `h` compares first, if
    /// its table slot (prefetched earlier with `prefetch_long`) holds one with the
    /// same tag.
    #[inline(always)]
    pub fn prefetch_long_string(h: u64) {
        #[cfg(target_arch = "x86_64")]
        if let Some(t) = LTAB.get() {
            let v = unsafe { t.slots.get_unchecked((h as usize) & t.mask) }.load(Ordering::Relaxed);
            if v > CLOSED && v & (0xFFFF << 48) == h & (0xFFFF << 48) {
                unsafe { core::arch::x86_64::_mm_prefetch((v & ((1 << 48) - 1)) as *const i8, core::arch::x86_64::_MM_HINT_T0) }
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = h;
    }

    /// `intern` for an ID of more than 15 bytes whose `long_hash` is `h`.
    #[inline(never)]
    pub fn intern_long(b: &[u8], h: u64) -> Id {
        debug_assert!(b.len() > 15);
        if let Some(p) = intern_table(b, h) {
            return Id { lo: p as u64, hi: 0xFF << 56 };
        }
        let mut g = LONG[(h >> 58) as usize].lock().unwrap();
        let map = g.get_or_insert_with(LongMap::default);
        let p = match map.get(b) {
            Some(&p) => p,
            None => {
                let mut v = Vec::with_capacity(8 + b.len());
                v.extend_from_slice(&(b.len() as u64).to_le_bytes());
                v.extend_from_slice(b);
                let s: &'static [u8] = Box::leak(v.into_boxed_slice());
                map.insert(&s[8..], s.as_ptr() as usize);
                s.as_ptr() as usize
            }
        };
        Id { lo: p as u64, hi: 0xFF << 56 }
    }

    /// The ID of the field `[s, e)` of `d` (16 bytes readable past `s`).
    #[inline(always)]
    pub unsafe fn field(d: *const u8, s: usize, e: usize) -> Id {
        let len = e - s;
        if len <= 15 {
            Id::short(d.add(s), len)
        } else {
            Id::intern(std::slice::from_raw_parts(d.add(s), len))
        }
    }

    /// A 128-bit key for an event ID, which only needs to be compared, never read
    /// back: short IDs inline (exact), long ones hashed.
    #[inline(always)]
    pub unsafe fn key(d: *const u8, s: usize, e: usize) -> Id {
        let len = e - s;
        if len <= 15 {
            return Id::short(d.add(s), len);
        }
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        if len == 33 && *d.add(s) == b'e' {
            // `e` + 32 lowercase hexadecimal digits (the generator's long event IDs):
            // the 128 bits they spell, a key that is distinct for distinct such IDs.
            if let Some((lo, hi)) = hex32(d.add(s + 1)) {
                return Id { lo, hi };
            }
        }
        Id::hashed(std::slice::from_raw_parts(d.add(s), len))
    }

    /// The key of an event ID of more than 15 bytes: its bytes eight at a time (the
    /// last, partial word zero-filled), two chained multiply-folds. Words are read
    /// in place (the partial one as the last eight bytes, shifted), never copied.
    #[inline(never)]
    fn hashed(b: &[u8]) -> Id {
        let n = b.len();
        let p = b.as_ptr();
        let (mut x, mut y) = (K3 ^ n as u64, K4);
        let mut step = |w: u64| {
            x = mum(x ^ w, K1).rotate_left(29) ^ w;
            y = mum(y ^ w, K2).rotate_left(23).wrapping_add(x);
        };
        let mut i = 0;
        while i + 8 <= n {
            step(unsafe { (p.add(i) as *const u64).read_unaligned() });
            i += 8;
        }
        if i < n {
            // n > 15, so the eight bytes ending at n are all inside b.
            let r = n - i;
            step(unsafe { (p.add(n - 8) as *const u64).read_unaligned() } >> (8 * (8 - r)));
        }
        Id { lo: mum(x, K1 ^ y), hi: mum(y ^ K3, x ^ K4) | (0xFE << 56) }
    }

    /// A click ID of more than 15 bytes by reference: its `len` (< 256) bytes stay
    /// where they are, in an input mapping that lives to process exit. Such an `Id`
    /// orders like its bytes (`less`), but equal strings at two places are two
    /// values: for click IDs, which are only ever ordered, never matched.
    #[inline(always)]
    pub fn external(p: *const u8, len: usize) -> Id {
        debug_assert!(len > 15 && len < 256);
        Id { lo: p as u64, hi: (0xFD << 56) | len as u64 }
    }

    /// Stored elsewhere: interned, or (marker 0xFD) by reference.
    #[inline(always)]
    pub fn is_long(&self) -> bool {
        (self.hi >> 56) | 2 == 0xFF
    }

    /// The bytes of the ID (not valid for event keys made by `key`).
    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        unsafe {
            if self.hi >> 56 == 0xFD {
                std::slice::from_raw_parts(self.lo as usize as *const u8, (self.hi & 0xFF) as usize)
            } else if self.is_long() {
                let p = self.lo as usize as *const u8;
                let n = (p as *const u64).read_unaligned() as usize;
                std::slice::from_raw_parts(p.add(8), n)
            } else {
                std::slice::from_raw_parts(self as *const Id as *const u8, (self.hi >> 56) as usize)
            }
        }
    }

    /// Byte-wise string order.
    #[inline(always)]
    pub fn less(&self, o: &Id) -> bool {
        if self.is_long() | o.is_long() {
            return self.bytes() < o.bytes();
        }
        // Inline: big-endian compare of the padded bytes, then the length.
        (self.lo.swap_bytes(), self.hi.swap_bytes()) < (o.lo.swap_bytes(), o.hi.swap_bytes())
    }

    /// Table hash.
    #[inline(always)]
    pub fn hash(&self) -> u64 {
        mum(self.lo ^ K1, self.hi ^ K2)
    }

    /// Two independent 64-bit hashes (event dedup: one is the stored identity,
    /// the other picks the bucket).
    #[inline(always)]
    pub fn hash2(&self) -> (u64, u64) {
        (mum(self.lo ^ K1, self.hi ^ K2), mum(self.lo ^ K3, self.hi ^ K4))
    }
}

#[cfg(test)]
mod tests {
    use super::Id;

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[test]
    fn hex_keys() {
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        let digits = b"0123456789abcdef";
        let mut seen = std::collections::HashMap::new();
        for _ in 0..20_000 {
            let mut t = [0u8; 64];
            t[0] = b'e';
            let mut want: u128 = 0;
            for i in 0..32 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let d = (x % 16) as usize;
                t[1 + i] = digits[d];
                want = want << 4 | d as u128;
            }
            let k = unsafe { Id::key(t.as_ptr(), 0, 33) };
            // Distinct hex strings give distinct keys.
            assert_eq!(*seen.entry((k.lo, k.hi)).or_insert(want), want);
            // Any byte that is not a lowercase hex digit falls back to the hashed key.
            for bad in [b'g', b'A', b'F', b'/', b':', b'`', b' '] {
                let mut u = t;
                u[1 + (x % 32) as usize] = bad;
                assert_eq!(unsafe { Id::key(u.as_ptr(), 0, 33) }, Id::hashed(&u[..33]));
            }
        }
    }

    #[test]
    fn long_keys_as_before() {
        // The word-at-a-time key against the chunk-copying one it replaced.
        fn old(b: &[u8]) -> Id {
            use super::{mum, K1, K2, K3, K4};
            let (mut x, mut y) = (K3 ^ b.len() as u64, K4);
            for c in b.chunks(8) {
                let mut w = [0u8; 8];
                w[..c.len()].copy_from_slice(c);
                let w = u64::from_le_bytes(w);
                x = mum(x ^ w, K1).rotate_left(29) ^ w;
                y = mum(y ^ w, K2).rotate_left(23).wrapping_add(x);
            }
            Id { lo: mum(x, K1 ^ y), hi: mum(y ^ K3, x ^ K4) | (0xFE << 56) }
        }
        let mut v: Vec<u8> = Vec::new();
        for n in 16..80 {
            v.push((n * 37 % 251) as u8);
            assert_eq!(Id::hashed(&v), old(&v), "length {}", v.len());
        }
    }

    #[test]
    fn inline_ids_order_like_bytes() {
        let mut x: u64 = 0x1234_5678_9abc_def1;
        let mut rnd = move |m: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % m
        };
        let mut strs: Vec<Vec<u8>> = Vec::new();
        for _ in 0..3000 {
            let n = rnd(22) as usize;
            let alpha = [b"ab\0\xff"[..].to_vec(), b"abcxyzAZ019 _".to_vec()];
            let a = &alpha[rnd(2) as usize];
            strs.push((0..n).map(|_| a[rnd(a.len() as u64) as usize]).collect());
        }
        for a in &strs {
            let ia = Id::intern(a);
            assert_eq!(ia.bytes(), &a[..]);
            for b in strs.iter().take(200) {
                let ib = Id::intern(b);
                assert_eq!(ia == ib, a == b);
                assert_eq!(ia.less(&ib), a < b, "{a:?} {b:?}");
            }
        }
        assert_eq!(Id::intern(b""), Id::EMPTY);
        // By reference: ordered like the bytes, against inline and interned IDs alike.
        for a in strs.iter().filter(|a| a.len() > 15).take(300) {
            let ea = Id::external(a.as_ptr(), a.len());
            assert_eq!(ea.bytes(), &a[..]);
            for b in strs.iter().take(200) {
                let ib = Id::intern(b);
                assert_eq!(ea.less(&ib), &a[..] < &b[..], "{a:?} {b:?}");
                assert_eq!(ib.less(&ea), &b[..] < &a[..], "{b:?} {a:?}");
            }
        }
    }
}
