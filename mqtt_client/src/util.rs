pub fn buf_with_size(n: usize) -> Vec<u8> {
    let mut vec: Vec<u8> = Vec::with_capacity(n);
    unsafe { vec.set_len(n) };
    vec
}
