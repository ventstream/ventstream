//! Lightweight DDL recognition for binlog `QueryEvent`s.
//!
//! Statement-level DDL (`ALTER`/`RENAME`/`DROP`/`CREATE TABLE`, `TRUNCATE`)
//! arrives in the binlog as raw query text. We only need to know *which
//! tables* changed so the schema cache can refetch before the next rows
//! event decodes against a stale layout — so this is a token matcher, not a
//! SQL parser. Anything it does not recognize is ignored.

/// The DDL statement kinds that affect row decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DdlKind {
    Alter,
    Rename,
    Drop,
    Create,
    Truncate,
}

/// A recognized DDL statement and every `(database, table)` it touches.
/// `RENAME TABLE a TO b` lists both names.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DdlStatement {
    pub(crate) kind: DdlKind,
    pub(crate) tables: Vec<(String, String)>,
}

/// Recognize a DDL statement. `default_db` fills in unqualified table names
/// (the binlog `QueryEvent` carries the session's current schema).
pub(crate) fn parse_ddl(sql: &str, default_db: &str) -> Option<DdlStatement> {
    let mut tokens = Tokenizer::new(sql);
    let first = tokens.next_word()?;
    match first.as_str() {
        "ALTER" => {
            tokens.expect_word("TABLE")?;
            let table = tokens.next_table_ident(default_db)?;
            Some(DdlStatement {
                kind: DdlKind::Alter,
                tables: vec![table],
            })
        }
        "RENAME" => {
            tokens.expect_word("TABLE")?;
            // RENAME TABLE a TO b [, c TO d]* — both sides are affected.
            let mut tables = Vec::new();
            loop {
                let from = tokens.next_table_ident(default_db)?;
                tokens.expect_word("TO")?;
                let to = tokens.next_table_ident(default_db)?;
                tables.push(from);
                tables.push(to);
                if !tokens.consume_comma() {
                    break;
                }
            }
            Some(DdlStatement {
                kind: DdlKind::Rename,
                tables,
            })
        }
        "DROP" => {
            tokens.expect_word("TABLE")?;
            tokens.consume_words(&["IF", "EXISTS"]);
            let mut tables = vec![tokens.next_table_ident(default_db)?];
            while tokens.consume_comma() {
                match tokens.next_table_ident(default_db) {
                    Some(t) => tables.push(t),
                    None => break,
                }
            }
            Some(DdlStatement {
                kind: DdlKind::Drop,
                tables,
            })
        }
        "CREATE" => {
            tokens.expect_word("TABLE")?;
            tokens.consume_words(&["IF", "NOT", "EXISTS"]);
            let table = tokens.next_table_ident(default_db)?;
            Some(DdlStatement {
                kind: DdlKind::Create,
                tables: vec![table],
            })
        }
        "TRUNCATE" => {
            // `TABLE` keyword is optional.
            let _ = tokens.consume_words(&["TABLE"]);
            let table = tokens.next_table_ident(default_db)?;
            Some(DdlStatement {
                kind: DdlKind::Truncate,
                tables: vec![table],
            })
        }
        _ => None,
    }
}

struct Tokenizer<'a> {
    rest: &'a str,
}

impl<'a> Tokenizer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            rest: input.trim_start(),
        }
    }

    fn skip_ws(&mut self) {
        self.rest = self.rest.trim_start();
    }

    /// Next bare keyword, uppercased. Stops at whitespace, comma, or paren.
    fn next_word(&mut self) -> Option<String> {
        self.skip_ws();
        let end = self
            .rest
            .find(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ';')
            .unwrap_or(self.rest.len());
        if end == 0 {
            return None;
        }
        let word = self.rest.get(..end)?.to_ascii_uppercase();
        self.rest = self.rest.get(end..).unwrap_or("");
        Some(word)
    }

    fn expect_word(&mut self, expected: &str) -> Option<()> {
        (self.next_word()? == expected).then_some(())
    }

    /// Consume the given word sequence if present; no-op otherwise.
    fn consume_words(&mut self, words: &[&str]) -> bool {
        let saved = self.rest;
        for w in words {
            if self.next_word().as_deref() != Some(*w) {
                self.rest = saved;
                return false;
            }
        }
        true
    }

    fn consume_comma(&mut self) -> bool {
        self.skip_ws();
        if let Some(stripped) = self.rest.strip_prefix(',') {
            self.rest = stripped;
            true
        } else {
            false
        }
    }

    /// Next table identifier: `table`, `db.table`, with optional backtick or
    /// double-quote quoting on either part. Returns `(db, table)`.
    fn next_table_ident(&mut self, default_db: &str) -> Option<(String, String)> {
        self.skip_ws();
        let first = self.next_ident_part()?;
        self.skip_ws();
        if let Some(stripped) = self.rest.strip_prefix('.') {
            self.rest = stripped;
            let second = self.next_ident_part()?;
            Some((first, second))
        } else {
            Some((default_db.to_owned(), first))
        }
    }

    fn next_ident_part(&mut self) -> Option<String> {
        self.skip_ws();
        let mut chars = self.rest.chars();
        match chars.next()? {
            quote @ ('`' | '"') => {
                let body = &self.rest[1..];
                let end = body.find(quote)?;
                let ident = body.get(..end)?.to_owned();
                self.rest = body.get(end + 1..).unwrap_or("");
                Some(ident)
            }
            _ => {
                let end = self
                    .rest
                    .find(|c: char| {
                        c.is_whitespace() || c == '.' || c == ',' || c == '(' || c == ';'
                    })
                    .unwrap_or(self.rest.len());
                if end == 0 {
                    return None;
                }
                let ident = self.rest.get(..end)?.to_owned();
                self.rest = self.rest.get(end..).unwrap_or("");
                Some(ident)
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn t(db: &str, table: &str) -> (String, String) {
        (db.to_owned(), table.to_owned())
    }

    #[test]
    fn recognizes_alter_with_and_without_db_prefix() {
        let s = parse_ddl("ALTER TABLE orders ADD COLUMN note TEXT", "shop").unwrap();
        assert_eq!(s.kind, DdlKind::Alter);
        assert_eq!(s.tables, vec![t("shop", "orders")]);

        let s = parse_ddl("alter table `crm`.`leads` drop column x", "shop").unwrap();
        assert_eq!(s.tables, vec![t("crm", "leads")]);
    }

    #[test]
    fn rename_lists_both_sides_and_chains() {
        let s = parse_ddl("RENAME TABLE a TO b, `c` TO `d`", "shop").unwrap();
        assert_eq!(s.kind, DdlKind::Rename);
        assert_eq!(
            s.tables,
            vec![
                t("shop", "a"),
                t("shop", "b"),
                t("shop", "c"),
                t("shop", "d")
            ]
        );
    }

    #[test]
    fn drop_handles_if_exists_and_lists() {
        let s = parse_ddl("DROP TABLE IF EXISTS orders, archive.orders_old", "shop").unwrap();
        assert_eq!(s.kind, DdlKind::Drop);
        assert_eq!(
            s.tables,
            vec![t("shop", "orders"), t("archive", "orders_old")]
        );
    }

    #[test]
    fn create_and_truncate_forms() {
        let s = parse_ddl("CREATE TABLE IF NOT EXISTS orders (id INT)", "shop").unwrap();
        assert_eq!(s.kind, DdlKind::Create);
        assert_eq!(s.tables, vec![t("shop", "orders")]);

        let s = parse_ddl("TRUNCATE orders", "shop").unwrap();
        assert_eq!(s.kind, DdlKind::Truncate);
        let s = parse_ddl("TRUNCATE TABLE `shop`.`orders`", "x").unwrap();
        assert_eq!(s.tables, vec![t("shop", "orders")]);
    }

    #[test]
    fn ignores_non_ddl_and_non_table_ddl() {
        assert!(parse_ddl("BEGIN", "shop").is_none());
        assert!(parse_ddl("INSERT INTO orders VALUES (1)", "shop").is_none());
        assert!(parse_ddl("CREATE INDEX idx ON orders (id)", "shop").is_none());
        assert!(parse_ddl("DROP INDEX idx ON orders", "shop").is_none());
        assert!(parse_ddl("ALTER USER 'x'", "shop").is_none());
        assert!(parse_ddl("", "shop").is_none());
    }
}
