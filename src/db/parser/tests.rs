
#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};
    use crate::loki_kv::loki_kv::LokiKV;
    use crate::parser::parser::{parse_lokiql, QLCommands, QLValues};
    use crate::parser::executor::Executor;

    #[test]
    fn test_parse_set_string() {
        let query = "SET key 'value';";
        let asts = parse_lokiql(query);
        assert_eq!(asts.len(), 1);
        let ast = asts[0].as_ref().unwrap();
        let command_node = ast.get_left_child().unwrap();
        assert!(matches!(command_node.get_value(), QLValues::QLCommand(QLCommands::SET)));
        let key_node = command_node.get_left_child().unwrap();
        assert!(matches!(key_node.get_value(), QLValues::QLId(s) if s == "key"));
        let value_node = command_node.get_right_child().unwrap();
        assert!(matches!(value_node.get_value(), QLValues::QLString(s) if s == "'value'"));
    }

    #[test]
    fn test_parse_set_int() {
        let query = "SET key 123;";
        let asts = parse_lokiql(query);
        assert_eq!(asts.len(), 1);
        let ast = asts[0].as_ref().unwrap();
        let command_node = ast.get_left_child().unwrap();
        assert!(matches!(command_node.get_value(), QLValues::QLCommand(QLCommands::SET)));
        let key_node = command_node.get_left_child().unwrap();
        assert!(matches!(key_node.get_value(), QLValues::QLId(s) if s == "key"));
        let value_node = command_node.get_right_child().unwrap();
        assert!(matches!(value_node.get_value(), QLValues::QLInt(123)));
    }

    #[test]
    fn test_parse_get() {
        let query = "GET key;";
        let asts = parse_lokiql(query);
        assert_eq!(asts.len(), 1);
        let ast = asts[0].as_ref().unwrap();
        let command_node = ast.get_left_child().unwrap();
        assert!(matches!(command_node.get_value(), QLValues::QLCommand(QLCommands::GET)));
        let key_node = command_node.get_left_child().unwrap();
        assert!(matches!(key_node.get_value(), QLValues::QLId(s) if s == "key"));
    }

    #[test]
    fn test_execute_set_get() {
        let db = Arc::new(RwLock::new(LokiKV::new()));
        let query = "SET key 'value';";
        let asts = parse_lokiql(query);
        let mut executor = Executor::new(db.clone(), asts);
        executor.execute();

        let query = "GET key;";
        let asts = parse_lokiql(query);
        let mut executor = Executor::new(db.clone(), asts);
        let result = executor.execute();

        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], crate::loki_kv::loki_kv::ValueObject::StringData(s) if s == "'value'"));
    }
}
