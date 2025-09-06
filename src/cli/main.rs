use std::{
    error::Error,
    io::{self, BufRead, BufReader, Read, Write},
    net::TcpStream,
};

use clap::Parser;
use paris::Logger;

#[derive(Parser)]
#[command(author, version, about, long_about=None)]
struct Args {
    #[arg(default_value = "localhost")]
    host: String,
    #[arg(default_value_t = 8765)]
    port: usize,
}

fn main() {
    let mut logger = Logger::new();
    let args = Args::parse();
    let s = format!("Connecting to {}:{}.....", args.host, args.port);
    logger.loading(s.as_str());
    let mut stream = match TcpStream::connect(format!("{}:{}", args.host, args.port)) {
        Ok(strm) => {
            logger.done();
            logger.success("Connected to LokiKV instance!");
            strm
        }
        Err(err) => panic!("Unable to connect! Error: {}", err),
    };

    let mut reader = BufReader::new(stream.try_clone().expect("Failed to clone stream"));
    let mut writer = stream;

    // Prints welcome message
    println!(
        "\n\
    ╔════════════════════════════════════════════════════╗\n\
    ║            🚀 Welcome to LokiKV v0.0.1-alpha! 🚀   ║\n\
    ╠════════════════════════════════════════════════════╣\n\
    ║ This is a test drive for this key-value store.     ║\n\
    ║ To learn more about supported commands, check out  ║\n\
    ║ our README:                                        ║\n\
    ║ 👉 https://github.com/destrex271/LokiKV            ║\n\
    ╠════════════════════════════════════════════════════╣\n\
    ║ 🛠 Found a bug? Raise an issue on GitHub!          ║\n\
    ║ 🐞 GitHub Issues:                                  ║\n\
    ║ 👉 https://github.com/destrex271/LokiKV/issues     ║\n\
    ╠════════════════════════════════════════════════════╣\n\
    ║ 🚀 New Features:                                   ║\n\
    ║    🔹 Persistence                                  ║\n\
    ║      🔹 Use `PERSIST colname` to save to disk      ║\n\
    ║      🔹 Use `LOAD_BCUST colname` to save to        ║\n\
    ║         custom btree                               ║\n\
    ║      🔹 Similarly use LOAD_BDEF and LOAD_HMAP      ║\n\
    ╠════════════════════════════════════════════════════╣\n\
    ║ 🚀 Upcoming Features:                              ║\n\
    ║    🔹 WAL & Snapshots                              ║\n\
    ║    🔹 Distributed storage (in-memory & persistent) ║\n\
    ╠════════════════════════════════════════════════════╣\n\
    ║ ✨ Developed by: Akshat Jaimini (destrex271)       ║\n\
    ╚════════════════════════════════════════════════════╝\n"
    );

    loop {
        print!(">>> ");
        io::stdout().flush().expect("Failed to flush stdout");

        let mut buf = String::new();
        if io::stdin().read_line(&mut buf).is_err() {
            logger.error("Couldn't read command");
            continue;
        }

        if !buf.ends_with("\n") {
            buf.push('\n');
        }

        // println!("Writing to stream: {}", buf);
        if let Err(e) = writer.write_all(buf.as_bytes()) {
            let e = format!("Failed to send command: {}", e);
            logger.error(e.as_str());
            break;
        }
        // println!("Written to stream!");

        // println!("Checking response....");
        let mut response = String::new();
        loop {
            let mut line = String::new();
            if let Ok(bytes) = reader.read_line(&mut line) {
                if bytes == 0 {
                    logger.error("Seems like there was an error! Please try again..");
                    return;
                }

                if line.trim() == "<END_OF_RESPONSE>" {
                    break; // Stop reading when marker is received
                }

                response.push_str(line.trim());
                response += "\n";
            } else {
                logger.error("Failed to read response.");
                break;
            }
        }

        let t = format!("{}", response);
        logger.info(t.as_str());
    }
}
