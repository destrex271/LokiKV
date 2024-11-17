mod server;
use std::{io::{stdin, stdout, Write}, thread::Result};
use lokikv::LokiKV;
use server::LokiServerFunctions;

fn main(){
    let serv = server::LokiServer::new("localhost".to_string(), 8765);
    serv.start_event_loop();
}

fn main_proc(){
    let mut store = LokiKV::new();
    let mut buffer_value = String::new();

    let mut command_history: Vec<String> = vec![];

    loop{
        let mut buf = String::new();
        print!(" >>> ");
        let _ = stdout().flush();
        stdin().read_line(&mut buf).expect("Unable to read from stdin!");
        buf = buf.replace('\n', "");

        // Tokenize input
        let commands = buf.split(";");
        for command in commands.into_iter(){
            command_history.push(command.to_string());
            if command.len() == 0{
                continue;
            }
            let dc = command.replace(";", "");
            let mut tokens = dc.split_whitespace();
            let cmd = tokens.nth(0);

            // Execute according to function
            match cmd.unwrap(){
                "SET" => {
                    // Check valid set command
                    let key: String = tokens.nth(0).unwrap().to_string();
                    println!("Key : {:?}", key);
                    let val: String = tokens.nth(0).unwrap().to_string();
                    store.put_generic(&key, &val);
                },
                "GET" => {
                    let key_wrap = tokens.nth(0);
                    let key = match key_wrap{
                        Some(data) => data.to_string(),
                        _ => "".to_string()
                    };
                    if key.len() == 0{
                        panic!("No key provided");
                    }
                    let arg_wrap = tokens.nth(0);
                    let arg = match arg_wrap{
                        Some(data) => data,
                        _ => "None"
                    };
                    let value = store.get_value(&key);
                    println!("VAL -> {}", value);
                    match arg{
                        "BUFFV" => {
                            buffer_value = value.clone();
                            println!("Buffer set to {:?}", buffer_value);
                        },
                        _ => {
                            buffer_value = String::new();
                        }
                    }
                },
                "ECHOBUFF" => {
                    println!("{:?}", buffer_value);
                },
                "INCR" => {
                    // Increases value at key
                    let key = tokens.nth(0).unwrap().to_string();
                    let val = store.get_value(&key);

                    // Try to convert variable to integer
                    match val.parse::<f64>(){
                        Ok(n) => {
                            // Update
                            let dc: f64 = n + 1.0;
                            println!("Updated value to : {}", dc);
                            store.put_generic(&key, &dc.to_string());
                        },
                        Err(_) => {
                            println!("[ERROR]: Data is not of NUMERIC type")
                        }
                    }
                },
                "DECR" => {
                    let key = tokens.nth(0).unwrap().to_string();
                    let val = store.get_value(&key);

                    // Try to convert variable to integer
                    match val.parse::<f64>(){
                        Ok(n) => {
                            // Update
                            let dc: f64 = n - 1.0;
                            store.put_generic(&key, &dc);
                        },
                        Err(_) => {
                            println!("[ERROR]: Data is not of NUMERIC type")
                        }
                    }

                }
                _ => println!("Not a valid command")
            }
        }
    }
}
