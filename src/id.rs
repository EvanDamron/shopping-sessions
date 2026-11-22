//! Identifiers (user, click, campaign, product IDs) as 16-byte values.
//!
//! An ID of up to 15 bytes is stored inline: its bytes, zero padding, and its
//! length in the last byte, so equal values mean equal strings. A longer ID is
//! interned once and stored as a pointer with the marker length 0xFF, so equal
//! values still mean equal strings. The empty string is all zeros.

use std::collections::HashMap;
use std::sync::Mutex;

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
        let shard = {
            use std::hash::Hasher;
            let mut h = LongHasher::default();
            h.write(b);
            (h.finish() >> 58) as usize
        };
        let mut g = LONG[shard].lock().unwrap();
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
            Id::short(d.add(s), len)
        } else {
            Id::hashed(std::slice::from_raw_parts(d.add(s), len))
        }
    }

    #[cold]
    #[inline(never)]
    fn hashed(b: &[u8]) -> Id {
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

    #[inline(always)]
    pub fn is_long(&self) -> bool {
        self.hi >> 56 == 0xFF
    }

    /// The bytes of the ID (not valid for event keys made by `key`).
    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        unsafe {
            if self.is_long() {
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
    }
}
