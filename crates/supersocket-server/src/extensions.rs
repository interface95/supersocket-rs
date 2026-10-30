use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt,
    sync::Mutex,
};

/// A map from type to value for attaching data to a session, such as a user id set by an
/// authentication middleware. Values are stored by type, so use a newtype to keep them apart.
#[derive(Default)]
pub struct Extensions {
    map: Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
}

impl Extensions {
    /// Stores `value`, returning the previous value of the same type.
    pub fn insert<T: Send + Sync + 'static>(&self, value: T) -> Option<T> {
        let previous = self
            .map
            .lock()
            .unwrap()
            .insert(TypeId::of::<T>(), Box::new(value));
        previous.and_then(|previous| previous.downcast().ok().map(|previous| *previous))
    }

    /// Returns a copy of the value of type `T`.
    pub fn get<T: Clone + Send + Sync + 'static>(&self) -> Option<T> {
        self.map
            .lock()
            .unwrap()
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref().cloned())
    }

    /// Runs `f` with a reference to the value of type `T`, for values that are not `Clone`. `f`
    /// must not use these extensions itself: they are locked while it runs.
    pub fn with<T: Send + Sync + 'static, R>(&self, f: impl FnOnce(Option<&T>) -> R) -> R {
        let map = self.map.lock().unwrap();
        f(map
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref()))
    }

    /// Removes and returns the value of type `T`.
    pub fn remove<T: Send + Sync + 'static>(&self) -> Option<T> {
        let removed = self.map.lock().unwrap().remove(&TypeId::of::<T>());
        removed.and_then(|removed| removed.downcast().ok().map(|removed| *removed))
    }

    /// Whether a value of type `T` is stored.
    pub fn contains<T: 'static>(&self) -> bool {
        self.map.lock().unwrap().contains_key(&TypeId::of::<T>())
    }
}

impl fmt::Debug for Extensions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Extensions")
            .field("len", &self.map.lock().unwrap().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct UserId(u64);

    #[test]
    fn stores_values_by_type() {
        let extensions = Extensions::default();
        assert_eq!(extensions.insert(UserId(1)), None);
        assert_eq!(extensions.insert(UserId(2)), Some(UserId(1)));
        assert_eq!(extensions.get::<UserId>(), Some(UserId(2)));
        assert_eq!(
            extensions.with(|id: Option<&UserId>| id.map(|id| id.0)),
            Some(2)
        );
        assert!(extensions.contains::<UserId>());
        assert_eq!(extensions.remove::<UserId>(), Some(UserId(2)));
        assert!(!extensions.contains::<UserId>());
    }
}
