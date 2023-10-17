/// Each chunk is either the first element, or the rest of the elements (marked as blanks)
pub(crate) enum Chunk<T> {
    First(T),
    Blank,
}
