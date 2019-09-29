import java.util.HashMap;
import java.util.Map;

public class DB {
    String name;
    Map<String, Table> tables;
    Table curTable;
    public DB (String name) {
        tables = new HashMap<>();
        this.name = name;
        curTable = null;
    }

    public Table createTable(String tableName, String[][] coloumAndType) {
        Table table = new Table(tableName, coloumAndType);
        tables.put(tableName, table);
        return table;
    }

    public String getDataBaseName() {
        return name;
    }

    public Table chooseTable(String tableName) {
        if (tables.containsKey(tableName)) {
            return tables.get(tableName);
        }
        System.out.println("the table is not existed");
        return null;
    }
}
