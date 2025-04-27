use core::panic;
use regex::{Regex, RegexBuilder};
use std::str::FromStr;

const DEBUG_PROC_MACRO: bool = match option_env!("DEBUG_PROC_MACRO") {
    Some(_) => true,
    None => false,
};

use proc_macro::{Delimiter, Group, Ident, Literal, TokenStream, TokenTree};

fn replace_read_exact(ident: Ident, params: Group) -> TokenStream {
    TokenStream::from_str(&format!("{ident}.read_exact({params}).await?")).unwrap()
}

fn replace_write_all(ident: Ident, params: Group) -> TokenStream {
    TokenStream::from_str(&format!("{ident}.write_all({params}).await?")).unwrap()
}

fn process_block(group: Group) -> TokenStream {
    //println!("body: {group}");
    let mut new_stream = TokenStream::new();
    let tokens: Vec<TokenTree> = group.stream().into_iter().collect();
    let mut skip = 0;
    for window in tokens.windows(5) {
        let t0 = &window[0];
        let t1 = &window[1];
        let t2 = &window[2];
        let t3 = &window[3];
        let t4 = &window[4];

        if skip > 0 {
            skip -= 1;
            continue;
        }
        // Check if matches pattern
        // <ident>.<ident>(<params>)?
        // e.g.
        // reader.read_exact(&mut buf)?
        if let (
            TokenTree::Ident(ident),
            TokenTree::Punct(p0),
            TokenTree::Ident(func),
            TokenTree::Group(params),
            TokenTree::Punct(p1),
        ) = (t0.clone(), t1.clone(), t2.clone(), t3.clone(), t4.clone())
        {
            if p0.to_string() == ".".to_string()
                && params.delimiter() == Delimiter::Parenthesis
                && p1.to_string() == "?".to_string()
            {
                let replacement = match func.to_string().as_str() {
                    "read_exact" => replace_read_exact(ident.clone(), params.clone()),
                    "write_all" => replace_write_all(ident.clone(), params.clone()),
                    _ => panic!("fail"),
                };
                println!(
                    "matched function: `{ident}{p0}{func}{params}{p1}`. Replacing with `{replacement}`"
                );
                new_stream.extend(replacement);
                skip += 4;
                continue;
            }
        }
        new_stream.extend(TokenStream::from_str(&t0.to_string()));
    }
    for token in &tokens[tokens.len().checked_sub(4).unwrap_or(0)..] {
        new_stream.extend(TokenStream::from_str(&token.to_string()).unwrap());
    }

    let tokens = new_stream;
    let mut new_stream = TokenStream::new();
    for token in tokens {
        let token_s = token.to_string();
        if let TokenTree::Group(group) = token {
            new_stream.extend(process_block(group));
            continue;
        }
        new_stream.extend(TokenStream::from_str(&token_s));
    }
    let mut fn_stream = TokenStream::new();
    fn_stream.extend([TokenTree::Group(Group::new(group.delimiter(), new_stream))]);
    fn_stream
}

#[proc_macro_attribute]
pub fn impl_async(_attr: TokenStream, mut item: TokenStream) -> TokenStream {
    let mut original = item.clone();
    let mut async_impl = TokenStream::new();
    for (i, token) in item.into_iter().enumerate() {
        // Add async
        if i == 1 {
            async_impl.extend(TokenStream::from_str("async").unwrap());
            async_impl.extend(TokenStream::from_str(&token.to_string()));
            continue;
        }
        // change name to <>_async
        else if i == 2 {
            if let TokenTree::Ident(punct) = &token {
                async_impl.extend(
                    TokenStream::from_str(&format!("{}_async", punct.to_string())).unwrap(),
                );
                continue;
            } else {
                panic!("Unexpected type {token:?} when expected function name.");
            }
        }
        //println!("root token: {token:?}");
        match &token {
            TokenTree::Group(group) => {
                if group.delimiter() == Delimiter::Parenthesis {
                    // Function args
                    async_impl.extend(
            TokenStream::from_str(&
            token
                .to_string()
                .replace(
                    "impl Read",
                    "(impl tokio::io::AsyncRead + tokio::io::AsyncReadExt + std::marker::Unpin)",
                )
                .replace(
                    "impl Write",
                    "(impl tokio::io::AsyncWrite +tokio::io::AsyncWriteExt+ std::marker::Unpin)",
                )).unwrap());
                } else {
                    async_impl.extend(process_block(group.clone()));
                }
            }
            TokenTree::Ident(ident) => async_impl.extend(TokenStream::from_str(
                &TokenTree::Ident(ident.clone()).to_string(),
            )),
            TokenTree::Punct(punct) => async_impl.extend(TokenStream::from_str(
                &TokenTree::Punct(punct.clone()).to_string(),
            )),
            TokenTree::Literal(literal) => {
                async_impl.extend(TokenStream::from_str(&TokenTree::Literal(()) literal.to_string()))
            }
        }
    }
    println!();
    println!();
    println!();
    println!("Original");
    println!();
    println!("{original}");
    println!();
    println!();
    println!("Async");
    println!();
    println!("{async_impl}");
    println!();
    println!();
    println!();
    println!();
    original.extend(async_impl);
    original
}

#[proc_macro_attribute]
pub fn impl_async2(_attr: TokenStream, mut item: TokenStream) -> TokenStream {
    let original = item.clone();
    let read_exact_re = RegexBuilder::new(r"read_exact\((.+?)\)\?\s*;")
        .multi_line(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap();
    let write_all_re = RegexBuilder::new(r"write_all\((.+?)\)\?\s*;")
        .multi_line(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap();
    let write_str_re = RegexBuilder::new(r"write_str\((.+?)\)\?\s*;")
        .multi_line(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap();
    let write_bytes_re = RegexBuilder::new(r"write_bytes\((.+?)\)\?\s*;")
        .multi_line(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap();
    let write_to_stream_re = RegexBuilder::new(r"[^pub fn ]write_to_stream\((.+?)\)\?\s*;")
        .multi_line(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap();

    let read_exact_re_test = Regex::new(r"read_exact\(").unwrap();
    let write_all_test = Regex::new(r"write_all\(").unwrap();
    let write_str_test = Regex::new(r"write_str\(").unwrap();
    let write_bytes_test = Regex::new(r"write_bytes\(").unwrap();
    let write_to_stream_test = Regex::new(r"[^pub fn ]write_to_stream\(").unwrap();

    let async_impl = item
        .to_string()
        .replace("pub fn try_read", "pub async try_read_async")
        .replace("pub fn write_to_stream", "pub async write_to_stream_async")
        .replace("pub fn write_str", "pub async fn write_str_async")
        .replace("pub fn write_bytes", "pub async fn write_bytes_async")
        .replace(
            "impl Read",
            "(impl tokio::io::AsyncRead + tokio::io::AsyncReadExt + std::marker::Unpin)",
        )
        .replace(
            "impl Write",
            "(impl tokio::io::AsyncWrite +tokio::io::AsyncWriteExt+ std::marker::Unpin)",
        );
    let async_impl = read_exact_re.replace_all(&async_impl, r"read_exact($1).await?;");
    let async_impl = write_all_re.replace_all(&async_impl, r"write_all($1).await?;");
    let async_impl = write_str_re.replace_all(&async_impl, r"write_str_async($1).await?;");
    let async_impl = write_bytes_re.replace_all(&async_impl, r"write_bytes_async($1).await?;");
    let async_impl =
        write_to_stream_re.replace_all(&async_impl, r"write_to_stream_async($1).await?;");
    let async_impl = TokenStream::from_str(&async_impl).unwrap();
    item.extend(async_impl.clone());

    if DEBUG_PROC_MACRO {
        println!("**********************");
        println!("**PROC MACRO START****");
        println!("**********************");
        println!("{item}");
        println!("**********************");
        println!("**/PROC MACRO STOP****");
        println!("**********************");
    }

    macro_rules! qa {
        ($re1:ident, $re2:ident, $error:literal) => {
            let got = $re1.find_iter(&original.to_string()).count();
            let expected = $re2.find_iter(&original.to_string()).count();
            if got != expected {
                println!($error);
                println!("Expected: {expected}");
                println!("Got: {got}");
                //panic!($error);
            }
        };
    }

    qa!(
        read_exact_re,
        read_exact_re_test,
        "Amount of `read_exact` did not match."
    );
    qa!(
        write_all_re,
        write_all_test,
        "Amount of `write_all` did not match."
    );
    qa!(
        write_str_re,
        write_str_test,
        "Amount of `write_str` did not match."
    );
    qa!(
        write_bytes_re,
        write_bytes_test,
        "Amount of `write_bytes` did not match."
    );
    qa!(
        write_to_stream_re,
        write_to_stream_test,
        "Amount of `write_to_stream` did not match."
    );

    item
}
