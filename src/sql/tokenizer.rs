use super::stream::{Location, Stream};
use super::token::{Keyword, Separator, Token};

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
        println!("tokenizing");
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
            return Ok(Token::EndOfFile);
        };

        println!("{}", chr);

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

    fn tokenize_keyword_or_identifier(&mut self) -> TokenizerResult {
        let value: String = self
            .stream
            .consume_while(Token::is_part_of_ident_or_keyword)
            .collect();

        // TODO: Use [phf](https://docs.rs/phf/) or something similar if this
        // keeps growing.
        let keyword = match value.to_uppercase().as_str() {
            "SELECT" => Keyword::Select,
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
    use super::Tokenizer;

    #[test]
    fn tokenize_select() {
        let sql = "SELECT name, age FROM people;";

        println!("{}", sql);

        let mut tok = Tokenizer::new(sql);

        println!("{:?}", tok);

        assert_eq!(tok.tokenize(), Ok(vec![]))
    }
}
