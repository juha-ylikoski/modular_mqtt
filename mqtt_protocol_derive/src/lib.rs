use core::panic;
use std::str::FromStr;

use proc_macro::{Delimiter, Group, TokenStream, TokenTree};

fn replace_func(func: &str, params: Group) -> TokenStream {
    TokenStream::from_str(&format!("{func}{params}.await")).unwrap()
}

fn process_3_len(stream: TokenStream) -> TokenStream {
    let mut new_stream = TokenStream::new();
    let tokens: Vec<TokenTree> = stream.into_iter().collect();
    let mut skip = 0;
    for window in tokens.windows(2) {
        let t0 = &window[0];
        let t1 = &window[1];

        if skip > 0 {
            skip -= 1;
            continue;
        }
        // Check if matches pattern
        // <ident>(<params>)
        // e.g.
        // writer_str(&mut writer)
        if let (TokenTree::Ident(func), TokenTree::Group(params)) = (t0.clone(), t1.clone()) {
            if params.delimiter() == Delimiter::Parenthesis {
                let replacement = match func.to_string().as_str() {
                    "read_exact" => replace_func("read_exact", params.clone()),
                    "write_all" => replace_func("write_all", params.clone()),
                    "write_to_stream" => replace_func("write_to_stream_async", params.clone()),
                    "write_str" => replace_func(
                        "crate::mqtt_protocol::util::write_str_async",
                        params.clone(),
                    ),
                    "write_bytes" => replace_func(
                        "crate::mqtt_protocol::util::write_bytes_async",
                        params.clone(),
                    ),
                    _ => {
                        new_stream.extend(TokenStream::from(t0.clone()));
                        continue;
                    }
                };
                //println!("matched function: `{func}{params}`. Replacing with `{replacement}`");
                new_stream.extend(replacement);
                skip = 1;
                continue;
            }
        }
        new_stream.extend(TokenStream::from(t0.clone()));
    }
    // Re-add missing tail
    if skip == 0 {
        if let Some(token) = tokens.last() {
            new_stream.extend(TokenStream::from(token.clone()));
        }
    }
    new_stream
}

fn process_block(group: Group) -> TokenStream {
    //println!("body: {group}");

    let tokens = process_3_len(group.stream());
    let mut new_stream = TokenStream::new();
    for token in tokens {
        if let TokenTree::Group(group) = &token {
            new_stream.extend(process_block(group.clone()));
            continue;
        }
        new_stream.extend(TokenStream::from(token));
    }
    let mut fn_stream = TokenStream::new();
    fn_stream.extend([TokenTree::Group(Group::new(group.delimiter(), new_stream))]);
    fn_stream
}

#[proc_macro_attribute]
pub fn impl_async(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut original = item.clone();
    let mut async_impl = TokenStream::new();
    for (i, token) in item.into_iter().enumerate() {
        //println!("token: {token:?}");
        // Add async
        if i == 1 {
            async_impl.extend(TokenStream::from_str("async").unwrap());
            async_impl.extend(TokenStream::from(token.clone()));
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
            TokenTree::Ident(_) => async_impl.extend(TokenStream::from(token.clone())),
            TokenTree::Punct(_) => async_impl.extend(TokenStream::from(token.clone())),
            TokenTree::Literal(_) => async_impl.extend(TokenStream::from(token.clone())),
        }
    }
    //println!();
    //println!();
    //println!();
    //println!("Original");
    //println!();
    //println!("{original}");
    //println!();
    //println!();
    //println!("Async");
    //println!();
    //println!("{async_impl}");
    //println!();
    //println!();
    //println!();
    //println!();
    original.extend(async_impl);
    original
}
