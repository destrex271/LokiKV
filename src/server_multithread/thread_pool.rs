use std::thread;
use std::sync::{mpsc, Arc, Mutex};

// Workers
struct Worker{
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker{
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker{
        let thread = thread::spawn(move || loop{ 
            let msg = receiver.lock().unwrap().recv();
            match msg{
                Ok(job) => {
                    println!("Worker {id} executing a job;");
                    job();
                    println!("Worker {id} executed job;")
                },
                Err(_) => {
                    println!("Worker {id} disconnected");
                    break;
                }
            }
        });

        Worker{id, thread: Some(thread)}
    }
}


// Thread Pool Logic
type Job = Box<dyn FnOnce() + Send + 'static>;
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>
}

impl ThreadPool{
    pub fn new(size: usize) -> ThreadPool{
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(size);
        for id in 0..size{
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }
        ThreadPool {workers, sender: Some(sender)}
    }
    pub fn execute<F>(&self, f:F)
    where 
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        println!("Sending Job");
        self.sender.as_ref().unwrap().send(job).unwrap();
        println!("Sent Job!");
    }
}

impl Drop for ThreadPool{
    fn drop(&mut self){
        drop(self.sender.take());
        for worker in &mut self.workers{
            println!("Shutting down worker {}", worker.id);
            if let Some(thread) = worker.thread.take(){
                thread.join().unwrap();
            }
        }
    }
}

