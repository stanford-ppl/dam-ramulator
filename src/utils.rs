pub(crate) trait VecUtils {
    type Item;
    fn filter_swap_retain<FilterType, TakeType>(&mut self, filter: FilterType, post: TakeType)
    where
        FilterType: FnMut(&mut Self::Item) -> bool,
        TakeType: FnMut(Self::Item);
}

impl<T> VecUtils for Vec<T> {
    type Item = T;
    fn filter_swap_retain<FilterType, TakeType>(
        &mut self,
        mut filter: FilterType,
        mut post: TakeType,
    ) where
        FilterType: FnMut(&mut Self::Item) -> bool,
        TakeType: FnMut(Self::Item),
    {
        let mut counter = 0;
        while counter < self.len() {
            let val = &mut self[counter];
            if !filter(val) {
                let taken = self.swap_remove(counter);
                post(taken);
            } else {
                counter += 1;
            }
        }
    }
}
