use super::stream::{Location, Stream};
use super::token::{Keyword, Separator, Token};
use chrono::prelude::*;

/// Stores both the [`Token`] and its starting location in the input string.
#[derive(PartialEq)]
pub(super) struct TokenWithLocation {
    pub variant: Token,
    pub location: Location,
}

impl TokenWithLocation {
    /// Discards the location. Used mostly for mapping:
    /// `.map(TokenWithLocation::token_only)`.
    pub fn token_only(self) -> Token {
        self.variant
    }

    /// Reference to [`Token`].
    pub fn token(&self) -> &Token {
        &self.variant
    }
}

#[derive(Debug)]
struct Tokenizer<'s> {
    stream: Stream<'s>,
    reached_eof: bool,
}

#[derive(Debug, PartialEq)]
pub(crate) enum TokenizerErrorKind {
    UnsupportedToken(char),
    UnexpectedWhileParsingOperator { unexpected: char, operator: Token },
    OperatorNotClosed(Token),
    InvalidNumberNotation,
    StringNotClosed,
    Other(String),
}
#[derive(Debug, PartialEq)]
pub(super) struct TokenizerError {
    pub kind: TokenizerErrorKind,
    pub location: Location,
    pub input: String,
}

type TokenizerResult = Result<Token, TokenizerError>;

impl<'s> Tokenizer<'s> {
    pub fn new(input: &'s str) -> Self {
        Self {
            stream: Stream::new(input),
            reached_eof: false,
        }
    }

    pub fn iter<'t>(&'t mut self) -> Iter<'t, 's> {
        self.into_iter()
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>, TokenizerError> {
        self.iter()
            .map(|result| result.map(TokenWithLocation::token_only))
            .collect()
    }

    fn optional_next_token_with_location(
        &mut self,
    ) -> Option<Result<TokenWithLocation, TokenizerError>> {
        if !self.reached_eof {
            Some(self.next_token_with_location())
        } else {
            None
        }
    }

    fn next_token_with_location(&mut self) -> Result<TokenWithLocation, TokenizerError> {
        let location = self.stream.current_location();

        self.next_token().map(|token| TokenWithLocation {
            variant: token,
            location,
        })
    }

    fn next_token(&mut self) -> TokenizerResult {
        let Some(chr) = self.stream.peek() else {
            self.reached_eof = true;
            return Ok(Token::EndOfFile);
        };

        match chr {
            ' ' => self.consume(Token::Separator(Separator::Space)),
            '\t' => self.consume(Token::Separator(Separator::Tab)),
            '\n' => self.consume(Token::Separator(Separator::LineBreak)),
            '\r' => self.consume(Token::Separator(Separator::CarriageReturn)),
            '<' => match self.stream.consume_and_peek() {
                Some('=') => self.consume(Token::LessThanOrEqual),
                _ => Ok(Token::LessThan),
            },
            '>' => match self.stream.consume_and_peek() {
                Some('=') => self.consume(Token::GreaterThanOrEqual),
                _ => Ok(Token::GreaterThan),
            },
            '*' => self.consume(Token::Multiply),
            '/' => self.consume(Token::Divide),
            '+' => self.consume(Token::Plus),
            '-' => self.consume(Token::Minus),
            '=' => self.consume(Token::Equal),
            '!' => match self.stream.consume_and_peek() {
                Some('=') => self.consume(Token::NotEqual),
                Some(unexpected) => {
                    let kind = TokenizerErrorKind::UnexpectedWhileParsingOperator {
                        unexpected: *unexpected,
                        operator: Token::NotEqual,
                    };
                    self.error(kind)
                }
                None => self.error(TokenizerErrorKind::OperatorNotClosed(Token::NotEqual)),
            },
            '(' => self.consume(Token::LeftParenthesis),
            ')' => self.consume(Token::RightParenthesis),
            ',' => self.consume(Token::Comma),
            ';' => self.consume(Token::Semicolon),
            '"' | '\'' => self.tokenize_string_or_datetime(),
            '0'..'9' => self.tokenize_number(),
            _ if Token::is_part_of_ident_or_keyword(chr) => self.tokenize_keyword_or_identifier(),
            _ => {
                let kind = TokenizerErrorKind::UnsupportedToken(*chr);
                self.error(kind)
            }
        }
    }

    fn consume(&mut self, token: Token) -> TokenizerResult {
        self.stream.next();
        Ok(token)
    }

    fn error(&self, kind: TokenizerErrorKind) -> TokenizerResult {
        Err(TokenizerError {
            kind,
            location: self.stream.current_location(),
            input: self.stream.input_string.to_owned(),
        })
    }

    fn tokenize_string_or_datetime(&mut self) -> TokenizerResult {
        let quote = self.stream.next().unwrap();

        let string: String = self.stream.consume_while(|chr| *chr != quote).collect();

        if self.stream.next().is_some_and(|chr| chr == quote) {
            // TODO - improve Datetime matching without needing the full RFC2822 string
            match string.parse::<DateTime<Utc>>() {
                Ok(_) => Ok(Token::Datetime(string)),
                Err(_) => Ok(Token::String(string)),
            }
        } else {
            self.error(TokenizerErrorKind::StringNotClosed)
        }
    }

    fn tokenize_number(&mut self) -> TokenizerResult {
        let string: String = self
            .stream
            .consume_while(|chr| {
                chr.is_ascii_digit()
                    || (*chr == '.')
                    || (*chr == '+')
                    || (*chr == '-')
                    || (*chr == 'e')
                    || (*chr == 'E')
            })
            .collect();

        let integer_parsing_result = string.parse::<i32>();
        let real_parsing_result = string.parse::<f32>();

        if integer_parsing_result.is_ok() && !string.contains(".") {
            Ok(Token::Integer(string))
        } else if real_parsing_result.is_ok() {
            Ok(Token::Real(string))
        } else {
            self.error(TokenizerErrorKind::InvalidNumberNotation)
        }
    }

    fn tokenize_keyword_or_identifier(&mut self) -> TokenizerResult {
        let value: String = self
            .stream
            .consume_while(Token::is_part_of_ident_or_keyword)
            .collect();

        // TODO: Use [phf](https://docs.rs/phf/) or something similar if this
        // keeps growing.
        let keyword = match value.to_uppercase().as_str() {
            "SELECT" => Keyword::Select,
            "FROM" => Keyword::From,
            "WHERE" => Keyword::Where,
            "GROUP" => Keyword::Group,
            "ORDER" => Keyword::Order,
            "BY" => Keyword::By,
            "INNER" => Keyword::Inner,
            "OUTER" => Keyword::Outer,
            "LEFT" => Keyword::Left,
            "RIGHT" => Keyword::Right,
            "JOIN" => Keyword::Join,
            "ON" => Keyword::On,
            _ => Keyword::None,
        };

        Ok(match keyword {
            Keyword::None => Token::Identifier(value),
            _ => Token::Keyword(keyword),
        })
    }
}

pub(super) struct Iter<'t, 's> {
    tokenizer: &'t mut Tokenizer<'s>,
}

impl<'t, 's> Iterator for Iter<'t, 's> {
    type Item = Result<TokenWithLocation, TokenizerError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokenizer.optional_next_token_with_location()
    }
}

impl<'t, 's> IntoIterator for &'t mut Tokenizer<'s> {
    type IntoIter = Iter<'t, 's>;
    type Item = Result<TokenWithLocation, TokenizerError>;

    fn into_iter(self) -> Self::IntoIter {
        Iter { tokenizer: self }
    }
}

pub(super) struct IntoIter<'s> {
    tokenizer: Tokenizer<'s>,
}

impl<'s> Iterator for IntoIter<'s> {
    type Item = Result<TokenWithLocation, TokenizerError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokenizer.optional_next_token_with_location()
    }
}

impl<'s> IntoIterator for Tokenizer<'s> {
    type IntoIter = IntoIter<'s>;
    type Item = Result<TokenWithLocation, TokenizerError>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { tokenizer: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_select() {
        let sql = "SELECT name, age FROM people;";

        let mut tok = Tokenizer::new(sql);

        assert_eq!(
            tok.tokenize(),
            Ok(vec![
                Token::Keyword(Keyword::Select),
                Token::Separator(Separator::Space),
                Token::Identifier("name".into()),
                Token::Comma,
                Token::Separator(Separator::Space),
                Token::Identifier("age".into()),
                Token::Separator(Separator::Space),
                Token::Keyword(Keyword::From),
                Token::Separator(Separator::Space),
                Token::Identifier("people".into()),
                Token::Semicolon,
                Token::EndOfFile,
            ])
        )
    }

    #[test]
    fn tokenize_select_with_integer_filter() {
        let sql = "SELECT date, value FROM samples WHERE value > 10;";

        let mut tok = Tokenizer::new(sql);

        assert_eq!(
            tok.tokenize(),
            Ok(vec![
                Token::Keyword(Keyword::Select),
                Token::Separator(Separator::Space),
                Token::Identifier("date".into()),
                Token::Comma,
                Token::Separator(Separator::Space),
                Token::Identifier("value".into()),
                Token::Separator(Separator::Space),
                Token::Keyword(Keyword::From),
                Token::Separator(Separator::Space),
                Token::Identifier("samples".into()),
                Token::Separator(Separator::Space),
                Token::Keyword(Keyword::Where),
                Token::Separator(Separator::Space),
                Token::Identifier("value".into()),
                Token::Separator(Separator::Space),
                Token::GreaterThan,
                Token::Separator(Separator::Space),
                Token::Integer("10".into()),
                Token::Semicolon,
                Token::EndOfFile,
            ])
        )
    }

    #[test]
    fn tokenize_select_with_real_float_filter() {
        let sql = "SELECT date, value FROM samples WHERE value > 10.0;";

        let mut tok = Tokenizer::new(sql);

        assert_eq!(
            tok.tokenize(),
            Ok(vec![
                Token::Keyword(Keyword::Select),
                Token::Separator(Separator::Space),
                Token::Identifier("date".into()),
                Token::Comma,
                Token::Separator(Separator::Space),
                Token::Identifier("value".into()),
                Token::Separator(Separator::Space),
                Token::Keyword(Keyword::From),
                Token::Separator(Separator::Space),
                Token::Identifier("samples".into()),
                Token::Separator(Separator::Space),
                Token::Keyword(Keyword::Where),
                Token::Separator(Separator::Space),
                Token::Identifier("value".into()),
                Token::Separator(Separator::Space),
                Token::GreaterThan,
                Token::Separator(Separator::Space),
                Token::Real("10.0".into()),
                Token::Semicolon,
                Token::EndOfFile,
            ])
        )
    }

    #[test]
    fn tokenize_select_with_real_scientific_filter() {
        let sql = "SELECT date, value FROM samples WHERE value > 1e+1;";

        let mut tok = Tokenizer::new(sql);

        assert_eq!(
            tok.tokenize(),
            Ok(vec![
                Token::Keyword(Keyword::Select),
                Token::Separator(Separator::Space),
                Token::Identifier("date".into()),
                Token::Comma,
                Token::Separator(Separator::Space),
                Token::Identifier("value".into()),
                Token::Separator(Separator::Space),
                Token::Keyword(Keyword::From),
                Token::Separator(Separator::Space),
                Token::Identifier("samples".into()),
                Token::Separator(Separator::Space),
                Token::Keyword(Keyword::Where),
                Token::Separator(Separator::Space),
                Token::Identifier("value".into()),
                Token::Separator(Separator::Space),
                Token::GreaterThan,
                Token::Separator(Separator::Space),
                Token::Real("1e+1".into()),
                Token::Semicolon,
                Token::EndOfFile,
            ])
        )
    }

    #[test]
    fn tokenize_select_with_datetime_filter() {
        let sql = "SELECT date, value FROM samples WHERE date > '2025-01-01T12:00:09Z';";

        let mut tok = Tokenizer::new(sql);

        assert_eq!(
            tok.tokenize(),
            Ok(vec![
                Token::Keyword(Keyword::Select),
                Token::Separator(Separator::Space),
                Token::Identifier("date".into()),
                Token::Comma,
                Token::Separator(Separator::Space),
                Token::Identifier("value".into()),
                Token::Separator(Separator::Space),
                Token::Keyword(Keyword::From),
                Token::Separator(Separator::Space),
                Token::Identifier("samples".into()),
                Token::Separator(Separator::Space),
                Token::Keyword(Keyword::Where),
                Token::Separator(Separator::Space),
                Token::Identifier("date".into()),
                Token::Separator(Separator::Space),
                Token::GreaterThan,
                Token::Separator(Separator::Space),
                Token::Datetime("2025-01-01T12:00:09Z".into()),
                Token::Semicolon,
                Token::EndOfFile,
            ])
        )
    }

    #[test]
    fn tokenize_select_with_string_filter() {
        let sql = "SELECT date, name FROM people WHERE name != 'Foo';";

        let mut tok = Tokenizer::new(sql);

        assert_eq!(
            tok.tokenize(),
            Ok(vec![
                Token::Keyword(Keyword::Select),
                Token::Separator(Separator::Space),
                Token::Identifier("date".into()),
                Token::Comma,
                Token::Separator(Separator::Space),
                Token::Identifier("name".into()),
                Token::Separator(Separator::Space),
                Token::Keyword(Keyword::From),
                Token::Separator(Separator::Space),
                Token::Identifier("people".into()),
                Token::Separator(Separator::Space),
                Token::Keyword(Keyword::Where),
                Token::Separator(Separator::Space),
                Token::Identifier("name".into()),
                Token::Separator(Separator::Space),
                Token::NotEqual,
                Token::Separator(Separator::Space),
                Token::String("Foo".into()),
                Token::Semicolon,
                Token::EndOfFile,
            ])
        )
    }
}
