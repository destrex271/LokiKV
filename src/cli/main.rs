use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about=None)]
struct Args{
    #[arg(default_value = "localhost")]
    host: String,
    #[arg(default_value_t = 8765)]
    port: usize
}

fn main(){
    let args = Args::parse();
    println!("Connecting to {}:{}!", args.host, args.port);
}
