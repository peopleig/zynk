use input_handler::InputHandler;
use std::path::PathBuf;
use zynk::engine::kv::LsmEngine;
use zynk::engine::crdt::ElementId;

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

            "gput" => {
                let k = match parts.next() {
                    Some(s) => s.as_bytes().to_vec(),
                    None => {
                        println!("usage: gadd <key> <element>");
                        continue;
                    }
                };
                let elem = match parts.next() {
                    Some(s) => s.as_bytes().to_vec(),
                    None => {
                        println!("usage: gadd <key> <element>");
                        continue;
                    }
                };
                if let Err(e) = engine.gset_add(k, elem) {
                    println!("error: {e}");
                } else {
                    println!("OK");
                }
            }

            "gget" => {
                let k = match parts.next() {
                    Some(s) => s.as_bytes(),
                    None => {
                        println!("usage: gget <key>");
                        continue;
                    }
                };
                match engine.gset_get(k) {
                    Ok(vs) => {
                        if vs.is_empty() {
                            println!("(empty set)");
                        } else {
                            for v in vs {
                                match std::str::from_utf8(&v) {
                                    Ok(s) => println!("{s}"),
                                    Err(_) => println!("0x{}", hex::encode(v)),
                                }
                            }
                        }
                    }
                    Err(e) => println!("error: {e}"),
                }
            }

            "ggetraw" => {
                let k = match parts.next() {
                    Some(s) => s.as_bytes(),
                    None => {
                        println!("usage: ggetraw <key>");
                        continue;
                    }
                };
                match engine.get(k) {
                    Ok(Some(v)) => {
                        println!("hex: {}", hex::encode(&v));
                        println!("raw: {:?}", v);
                    }
                    Ok(None) => println!("(nil)"),
                    Err(e) => println!("error: {e}"),
                }
            }

            "rga_insert" => {
                let k = match parts.next() {
                    Some(s) => s.as_bytes(),
                    None => {
                        println!("usage: rga_insert <key> <value>");
                        continue;
                    }
                };
                let v = match parts.next() {
                    Some(s) => s.as_bytes().to_vec(),
                    None => {
                        println!("usage: rga_insert <key> <value>");
                        continue;
                    }
                };
                let id = engine.next_element_id();
                println!("Generated actor id: {}", engine.actor_id);
                println!("Generated counter id: {}", id.counter);
                if let Err(e) = engine.rga_insert_after(k, None, v, engine.actor_id, id.counter) {
                    println!("error: {e}");
                } else {
                    println!("OK (id = actor:{} counter:{})", id.actor, id.counter);
                }
            }
            
            "rga_insert_after" => {
                let k = match parts.next() {
                    Some(s) => s.as_bytes(),
                    None => {
                        println!("usage: rga_insert_after <key> <prev_actor> <prev_counter> <value>");
                        continue;
                    }
                };
                let prev_actor = match parts.next() {
                    Some(s) => match s.parse::<u64>() {
                        Ok(v) => v,
                        Err(_) => {
                            println!("invalid prev_actor");
                            continue;
                        }
                    },
                    None => {
                        println!("usage: rga_insert_after <key> <prev_actor> <prev_counter> <value>");
                        continue;
                    }
                };
                let prev_counter = match parts.next() {
                    Some(s) => match s.parse::<u64>() {
                        Ok(v) => v,
                        Err(_) => {
                            println!("invalid prev_counter");
                            continue;
                        }
                    },
                    None => {
                        println!("usage: rga_insert_after <key> <prev_actor> <prev_counter> <value>");
                        continue;
                    }
                };
                let value = match parts.next() {
                    Some(s) => s.as_bytes().to_vec(),
                    None => {
                        println!("missing <value>");
                        continue;
                    }
                };
            
                let prev = Some(ElementId { actor: prev_actor, counter: prev_counter });
                let id = engine.next_element_id();
            
                if let Err(e) = engine.rga_insert_after(k, prev, value, engine.actor_id, id.counter) {
                    println!("error: {e}");
                } else {
                    println!("OK (id = actor:{} counter:{})", id.actor, id.counter);
                }
            }
            
            "rga_delete" => {
                let k = match parts.next() {
                    Some(s) => s.as_bytes(),
                    None => {
                        println!("usage: rga_delete <key> <actor> <counter>");
                        continue;
                    }
                };
                let actor = match parts.next() {
                    Some(s) => match s.parse::<u64>() {
                        Ok(v) => v,
                        Err(_) => {
                            println!("invalid actor id");
                            continue;
                        }
                    },
                    None => {
                        println!("usage: rga_delete <key> <actor> <counter>");
                        continue;
                    }
                };
                let counter = match parts.next() {
                    Some(s) => match s.parse::<u64>() {
                        Ok(v) => v,
                        Err(_) => {
                            println!("invalid counter");
                            continue;
                        }
                    },
                    None => {
                        println!("usage: rga_delete <key> <actor> <counter>");
                        continue;
                    }
                };
            
                let id = ElementId { actor, counter };
                if let Err(e) = engine.rga_delete(k, id) {
                    println!("error: {e}");
                } else {
                    println!("OK (deleted id = actor:{} counter:{})", actor, counter);
                }
            }
            
            "rga_show" => {
                let k = match parts.next() {
                    Some(s) => s.as_bytes(),
                    None => {
                        println!("usage: rga_show <key>");
                        continue;
                    }
                };
                match engine.rga_get_visible(k) {
                    Ok(vs) => {
                        if vs.is_empty() {
                            println!("(empty)");
                        } else {
                            for v in vs {
                                match std::str::from_utf8(&v) {
                                    Ok(s) => println!("{s}"),
                                    Err(_) => println!("0x{}", hex::encode(v)),
                                }
                            }
                        }
                    }
                    Err(e) => println!("error: {e}"),
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
