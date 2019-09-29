import java.util.List;
import java.util.Map;

public interface TableInterface {
    public boolean insertRow(String[][] newRow);
    public boolean deleteRow(int rowKey);
    public boolean updateRow(int rowKey, String[] updatedMessage);
    public Map<Integer, DbRow> getAllRows();
    public Map<Integer, DbRow> firstKRows(int k);
    public List<DbRow> sortBy(String coloumName, int k);
    public List<Object[]> groupBy(String coloumName, int k);
    public String getName();
}
