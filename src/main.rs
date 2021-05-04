use std::io::Write;

use anyhow::Result;

use relly::buffer::{BufferPool, BufferPoolManager};
use relly::disk::DiskManager;
use relly::lang::Request;

fn main() -> Result<()> {
    let mut args = std::env::args();
    args.next().expect("executable name");
    let heap_file_name = args.next().expect("no heap file is given");
    let disk = DiskManager::open(&heap_file_name)?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);
    let stdin = std::io::stdin();
    let stdin = stdin.lock();
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    let stream = serde_json::Deserializer::from_reader(stdin).into_iter::<Request>();
    stdout.write_all(b"> ")?;
    stdout.flush()?;
    for req in stream {
        if let Err(e) = handle_request(&mut bufmgr, req) {
            eprintln!("{}", e);
        }
        stdout.write_all(b"> ")?;
        stdout.flush()?;
    }
    bufmgr.flush()?;
    Ok(())
}

fn handle_request(
    bufmgr: &mut BufferPoolManager,
    req: Result<Request, serde_json::Error>,
) -> Result<()> {
    let req = req?;
    let res = req.execute(bufmgr)?;
    print!("{}", res);
    Ok(())
}
