use std;
use std::ops::Range;

pub fn par_partition_range_by_key_inner<'a, K, V, F>(
    base: usize,
    v: &'a [V],
    f: &F,
) -> Vec<(K, Range<usize>)>
where
    V: Sync,
    K: Eq + Send,
    F: Fn(&V) -> K + Send + Sync,
{
    if v.is_empty() {
        return Vec::new();
    }
    let mut half = v.len() / 2;
    let half_key = f(&v[half]);
    while half < v.len() && half_key == f(&v[half]) {
        half += 1;
    }

    if half == v.len() {
        // failed to split, run serial version
        let mut partitions = partition_range_by_key(v, f);
        for r in partitions.iter_mut() {
            r.1.start += base;
            r.1.end += base;
        }
        partitions
    } else {
        let (left_v, right_v) = v.split_at(half);
        let (mut left, mut right) = ::rayon::join(
            || par_partition_range_by_key_inner(base, left_v, f),
            || par_partition_range_by_key_inner(base + half, right_v, f),
        );

        left.append(&mut right);
        left
    }
}

/// `par_partition_range_by_key` returns range of slice which items have same key.
pub fn par_partition_range_by_key<'a, K, V, F>(v: &'a [V], f: F) -> Vec<(K, Range<usize>)>
where
    V: Sync,
    K: Eq + Send,
    F: Fn(&V) -> K + Send + Sync,
{
    par_partition_range_by_key_inner(0, v, &f)
}

/// `partition_range_by_key` returns range of slice which items have same key.
pub fn partition_range_by_key<'a, K, V, F>(v: &'a [V], f: F) -> Vec<(K, Range<usize>)>
where
    K: Eq,
    F: Fn(&V) -> K,
{
    if v.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut start_idx = 0usize;
    let mut cur_id = f(&v[start_idx]);
    for (i, ref item) in v.iter().enumerate() {
        let id = f(item);
        if cur_id != id {
            out.push((cur_id, start_idx..i));
            cur_id = id;
            start_idx = i;
        }
    }
    if start_idx != v.len() {
        out.push((cur_id, start_idx..v.len()));
    }

    out
}

/// `partition_by_key` returns vector of slice which items have same key.
pub fn partition_by_key<'a, K, V, F>(vec: &'a [V], f: F) -> Vec<(K, &'a [V])>
where
    V: Sync,
    K: Eq + Send,
    F: Fn(&V) -> K + Send + Sync,
{
    par_partition_range_by_key(vec, f)
        .into_iter()
        .map(|(k, r)| (k, &vec[r]))
        .collect()
}

/// `partition_mut_by_key` returns vector of mutable slice which items have same key.
pub fn partition_mut_by_key<'a, K, V, F>(v: &'a mut [V], f: F) -> Vec<(K, &'a mut [V])>
where
    V: Sync,
    K: Eq + Send,
    F: Fn(&V) -> K + Send + Sync,
{
    par_partition_range_by_key(v, f)
        .into_iter()
        .map(|(k, r)| {
            // There's no safe way to do this. Use unsafe functions to bypass lifetime/burrowck.
            let ptr = &mut v[r.start] as *mut V;
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, r.len()) };
            (k, slice)
        })
        .collect()
}
