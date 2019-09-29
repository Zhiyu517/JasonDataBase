import java.util.HashMap;
import java.util.Map;

public class DataBaseJason {

    Map<String, DB> DBs;
    public DataBaseJason() {
        DBs = new HashMap<>();
    }

    public DB createDataBase(String name) {
        DB db = new DB(name);
        DBs.put(name, db);
        return db;
    }

    public DB chooseDataBase(String dbName) {
        if (DBs.containsKey(dbName)) {
            return DBs.get(dbName);
        }
        System.out.println("The database is not existed");
        return null;
    }
}
