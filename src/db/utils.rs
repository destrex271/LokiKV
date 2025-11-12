use paris::Logger;

pub fn info(msg: &str) {
    let mut logger = Logger::new();
    logger.info(msg);
}

pub fn error(msg: &str) {
    let mut logger = Logger::new();
    logger.error(msg);
}

pub fn warning(msg: &str) {
    let mut logger = Logger::new();
    logger.warn(msg);
}

pub fn success(msg: &str) {
    let mut logger = Logger::new();
    logger.success(msg);
}

pub fn info_string(msg: String) {
    let mut logger = Logger::new();
    logger.info(msg.as_str());
}

pub fn error_string(msg: String) {
    let mut logger = Logger::new();
    logger.error(msg.as_str());
}

pub fn warning_string(msg: String) {
    let mut logger = Logger::new();
    logger.warn(msg.as_str());
}

pub fn success_string(msg: String) {
    let mut logger = Logger::new();
    logger.success(msg.as_str());
}
