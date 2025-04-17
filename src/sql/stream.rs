// Stream of chars with some helpers

use std::iter;
use std::str;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Location {
    pub line: usize,
    pub column: usize,
}

impl Default for Location {
    fn default() -> Self {
        Self { line: 1, column: 1 }
    }
}

pub(super) struct Stream<'s> {
    input_string: &'s str,
    current_location: Location,
    content: iter::Peekable<str::Chars<'s>>,
}

impl<'s> Stream<'s> {
    pub fn new(input_string: &'s str) -> Self {
        Self {
            input_string,
            current_location: Location::default(),
            content: input_string.chars().peekable(),
        }
    }

    pub fn next(&mut self) -> Option<char> {
        self.content.next().inspect(|c| match *c {
            '\n' => {
                self.current_location.line += 1;
                self.current_location.column = 1;
            }
            _ => {
                self.current_location.column += 1;
            } // is it worth to add support for explicit \r here?
        })
    }

    pub fn peek(&mut self) -> Option<&char> {
        self.content.peek()
    }

    // maybe add peek_and_consume ?
    fn consume_and_peek(&mut self) -> Option<&char> {
        self.next();
        self.peek()
    }

    fn current_location(&self) -> Location {
        self.current_location
    }

    fn consume_while<P: FnMut(&char) -> bool>(
        &mut self,
        consuming_condition: P,
    ) -> SafeTakeWhile<'_, 's, P> {
        SafeTakeWhile {
            stream: self,
            consuming_condition,
        }
    }
}

struct SafeTakeWhile<'t, 's, P> {
    stream: &'t mut Stream<'s>,
    consuming_condition: P,
}

impl<'t, 'c, P: FnMut(&char) -> bool> Iterator for SafeTakeWhile<'t, 'c, P> {
    type Item = char;

    fn next(&mut self) -> Option<Self::Item> {
        if (self.consuming_condition)(self.stream.peek()?) {
            self.stream.next()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construct_stream() {
        let input_string = "Hello, world!";
        let stream = Stream::new(&input_string);
        assert_eq!(input_string, stream.input_string);
        assert_eq!(Location::default(), stream.current_location());
    }

    #[test]
    fn test_stream_next() {
        let mut stream = Stream::new("Hello, world!");
        assert_eq!(stream.next(), Some('H'));
        assert_eq!(stream.next(), Some('e'));
        assert_eq!(stream.next(), Some('l'));
        assert_eq!(stream.next(), Some('l'));
        assert_eq!(stream.next(), Some('o'));
        assert_eq!(stream.next(), Some(','));
        assert_eq!(stream.next(), Some(' '));
        assert_eq!(stream.next(), Some('w'));
        assert_eq!(stream.next(), Some('o'));
        assert_eq!(stream.next(), Some('r'));
        assert_eq!(stream.next(), Some('l'));
        assert_eq!(stream.next(), Some('d'));
        assert_eq!(stream.next(), Some('!'));
        assert_eq!(stream.next(), None);
    }

    #[test]
    fn test_stream_peek() {
        let mut stream = Stream::new("Hello, world!");
        assert_eq!(stream.peek(), Some('H').as_ref());
        assert_eq!(stream.peek(), Some('H').as_ref());
    }

    #[test]
    fn test_stream_consume_and_peek() {
        let mut stream = Stream::new("Hello, world!");
        assert_eq!(stream.consume_and_peek(), Some('e').as_ref());
        assert_eq!(stream.consume_and_peek(), Some('l').as_ref());
        assert_eq!(stream.consume_and_peek(), Some('l').as_ref());
        assert_eq!(stream.consume_and_peek(), Some('o').as_ref());
        assert_eq!(stream.consume_and_peek(), Some(',').as_ref());
        assert_eq!(stream.consume_and_peek(), Some(' ').as_ref());
        assert_eq!(stream.consume_and_peek(), Some('w').as_ref());
        assert_eq!(stream.consume_and_peek(), Some('o').as_ref());
        assert_eq!(stream.consume_and_peek(), Some('r').as_ref());
        assert_eq!(stream.consume_and_peek(), Some('l').as_ref());
        assert_eq!(stream.consume_and_peek(), Some('d').as_ref());
        assert_eq!(stream.consume_and_peek(), Some('!').as_ref());
        assert_eq!(stream.consume_and_peek(), None);
    }

    #[test]
    fn test_stream_current_location() {
        let mut stream = Stream::new("Hello\n world!");
        assert_eq!(stream.current_location(), Location::default());
        stream.next();
        assert_eq!(stream.current_location(), Location { line: 1, column: 2 });
        stream.next();
        stream.next();
        stream.next();
        stream.next();
        stream.next();
        assert_eq!(stream.current_location(), Location { line: 2, column: 1 });
    }

    #[test]
    fn test_stream_consume_while() {
        let mut stream = Stream::new("Hello\n world!");
        for _ in stream.consume_while(|p| *p != '\n') {}
        assert_eq!(stream.current_location(), Location { line: 1, column: 6 });
    }
}
