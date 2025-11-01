use input_handler::InputHandler;
use zynk::engine::kv::LsmEngine;
use std::path::PathBuf;

fn main() {
    let mut engine = LsmEngine::new_with_manifest("data", 64 * 1024, 8 * 1024).expect("engine");

    let mut ih = InputHandler::with_history_file(PathBuf::from("data/history")).expect("input");

    println!("Zynk LSM KV. Commands: put/get/del/flush/exit");

    while let Ok(line) = ih.readline("zynk> ") {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(3, ' ');
        let cmd = parts.next().unwrap().to_lowercase();
        match cmd.as_str() {
            "put" => {
                let k = match parts.next() {
                    Some(s) => s.as_bytes(),
                    None => {
                        println!("usage: put <key> <value>");
                        continue;
                    }
                };
                let v = match parts.next() {
                    Some(s) => s.as_bytes(),
                    None => {
                        println!("usage: put <key> <value>");
                        continue;
                    }
                };
                if let Err(e) = engine.put(k, v) {
                    println!("error: {e}");
                } else {
                    println!("OK");
                }
            }
            "get" => {
                let k = match parts.next() {
                    Some(s) => s.as_bytes(),
                    None => {
                        println!("usage: get <key>");
                        continue;
                    }
                };
                match engine.get(k) {
                    Ok(Some(v)) => match std::str::from_utf8(&v) {
                        Ok(s) => println!("{s}"),
                        Err(_) => println!("0x{}", hex::encode(v)),
                    },
                    Ok(None) => println!("(nil)"),
                    Err(e) => println!("error: {e}"),
                }
            }
            "del" | "delete" => {
                let k = match parts.next() {
                    Some(s) => s.as_bytes(),
                    None => {
                        println!("usage: del <key>");
                        continue;
                    }
                };
                if let Err(e) = engine.delete(k) {
                    println!("error: {e}");
                } else {
                    println!("1");
                }
            }
            "flush" => {
                if let Err(e) = engine.flush() {
                    println!("error: {e}");
                } else {
                    println!("flushed");
                }
            }
            "exit" | "quit" => {
                println!("bye");
                break;
            }
            _ => println!("unknown command: {cmd}"),
        }
    }
}
