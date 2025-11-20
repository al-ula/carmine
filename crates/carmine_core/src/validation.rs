use crate::{
    Number,
    database::{KeyType, KeyTypes},
    error::{Error, Result},
};

pub fn validate_store_name(name: &str) -> Result<String> {
    validate_name(name, "database")?;
    Ok(name.to_string())
}

pub fn validate_collection_name(name: &str) -> Result<String> {
    // Internal collections are allowed to contain :: and __ but we need to check if it is a valid internal collection format if it does.
    // However, the spec says "Collections created by the engine (not user-facing)".
    // The user-facing API should probably reject :: and __ unless we have a specific way to allow internal creation.
    // For now, let's strictly follow the user-facing rules for general validation,
    // and maybe allow a bypass for internal use if needed, or just check the pattern.

    // The spec says:
    // Reserved: Cannot contain :: or __
    // Internal Collections: {collection}::__{suffix}

    // If the name contains :: or __, it must be a valid internal collection.
    // But wait, the prompt says "only implement for store and collection!", and the spec says "Internal Collections: Collections created by the engine".
    // Users shouldn't be able to create them via public API.
    // So for user input validation, we should reject them.

    validate_name(name, "collection")?;
    Ok(name.to_string())
}

fn validate_name(name: &str, context: &str) -> Result<()> {
    if name.is_empty() || name.len() > 64 {
        return Err(Error::Validation(format!(
            "{} name must be 1-64 characters",
            context
        )));
    }

    let mut chars = name.chars();
    if let Some(first) = chars.next() {
        if !first.is_ascii_alphabetic() && first != '_' {
            return Err(Error::Validation(format!(
                "{} name must start with letter or underscore",
                context
            )));
        }
    } else {
        // Empty string case handled by len check above, but just in case
        return Err(Error::Validation(format!(
            "{} name must be 1-64 characters",
            context
        )));
    }

    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' && c != '-' {
            return Err(Error::Validation(format!(
                "{} name contains invalid characters (allowed: a-z A-Z 0-9 _ -)",
                context
            )));
        }
    }

    if name.contains("::") {
        return Err(Error::Validation(format!(
            "{} name cannot contain '::' (reserved for internal use)",
            context
        )));
    }

    if name.contains("__") {
        return Err(Error::Validation(format!(
            "{} name cannot contain '__' (reserved for internal use)",
            context
        )));
    }

    Ok(())
}

pub trait KeyValidator: KeyType {
    fn validate(&self) -> Result<()>;
}

impl KeyValidator for String {
    fn validate(&self) -> Result<()> {
        if self.is_empty() || self.len() > 1024 {
            return Err(Error::Validation(format!("Key must be 1-1024 characters")));
        }
        if self.contains("::") {
            return Err(Error::Validation(format!(
                "Key cannot contain '::' (reserved for internal use)"
            )));
        }
        if self.contains("__") {
            return Err(Error::Validation(format!(
                "Key cannot contain '__' (reserved for internal use)"
            )));
        }
        // check if contain control characters
        if self.chars().any(|c| c.is_control()) {
            return Err(Error::Validation(format!(
                "Key cannot contain control characters"
            )));
        }
        Ok(())
    }
}

impl KeyValidator for i64 {
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

impl KeyValidator for Number {
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

impl KeyValidator for &[u8] {
    fn validate(&self) -> Result<()> {
        if self.is_empty() || self.len() > 1024 {
            return Err(Error::Validation(format!("Key must be 1-1024 characters")));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_store_name() {
        assert!(validate_store_name("prod").is_ok());
        assert!(validate_store_name("test_db").is_ok());
        assert!(validate_store_name("analytics-v2").is_ok());
        assert!(validate_store_name("_internal").is_ok());

        assert!(validate_store_name("123db").is_err());
        assert!(validate_store_name("my::db").is_err());
        assert!(validate_store_name("test__meta").is_err());
        assert!(validate_store_name("my-db!").is_err());
        assert!(validate_store_name("").is_err());
        assert!(validate_store_name("a".repeat(65).as_str()).is_err());
    }

    #[test]
    fn test_validate_collection_name() {
        assert!(validate_collection_name("users").is_ok());
        assert!(validate_collection_name("user_profiles").is_ok());
        assert!(validate_collection_name("cache-v2").is_ok());
        assert!(validate_collection_name("_private_data").is_ok());

        assert!(validate_collection_name("test::users").is_err());
        assert!(validate_collection_name("my__collection").is_err());
        assert!(validate_collection_name("1users").is_err());
        assert!(validate_collection_name("my.collection").is_err());
    }
}
