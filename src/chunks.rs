/// Each chunk is either the first element, or the rest of the elements (marked as blanks)
pub(crate) enum Chunk<T> {
    First(T),
    Blank,
}

impl<T> Chunk<T> {
    pub fn unwrap(&self) -> &T {
        if let Chunk::First(x) = self {
            x
        } else {
            panic!("Tried to unwrap a blank value!")
        }
    }

    pub fn is_blank(&self) -> bool {
        if let Chunk::Blank = self {
            true
        } else {
            false
        }
    }
}
