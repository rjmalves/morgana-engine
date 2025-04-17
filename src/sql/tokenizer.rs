use super::stream::{Location, Stream};
use super::token::{Keyword, Separator, Token};

struct Tokenizer<'s> {
    stream: Stream<'s>,
    tokens: Vec<Token>,
}

#[derive(PartialEq)]
pub(crate) enum TokenizerErrorKind {
    UnsupportedToken(char),
    UnexpectedWhileParsingOperator { unexpected: char, operator: Token },
    OperatorNotClosed(Token),
    StringNotClosed,
    Other(String),
}
#[derive(PartialEq)]
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
            tokens: vec![],
        }
    }

    fn next_token(&mut self) -> TokenizerResult {
        let Some(chr) = self.stream.peek() else {
            return Ok(Token::EndOfFile);
        };

        match chr {
            ' ' => self.consume(Token::Separator(Separator::Space)),
            '\t' => self.consume(Token::Separator(Separator::Tab)),
            _ => {
                let kind = TokenizerErrorKind::UnexpectedWhileParsingOperator { unexpected: (), operator: () }
            }
        }
    }

    fn consume(&mut self, token: Token) -> TokenizerResult {
        self.stream.next();
        Ok(token)
    }
}
