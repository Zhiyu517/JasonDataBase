import java.util.HashMap;
import java.util.Map;

public class DbRow {
    private Map<String, String> coloumNameAndType;
    private Map<String, String> row;
    public DbRow(Map<String, String> coloumNameAndTypeInPut) {
        coloumNameAndType = new HashMap<>(coloumNameAndTypeInPut);
        row = new HashMap<>();
    }

    public boolean createRow(String[][] newRow, int rowKey) {
        if (newRow.length == 0) {
            System.out.println("the input is not empty");
            return false;
        }
        for (int i = 0; i < newRow.length; i++) {
            String columnName = newRow[i][0];
            String value = newRow[i][1];
            if (!checkValid(columnName, value)) {
                return false;
            }
            row.put(columnName, value);
        }
        row.put("rowKey", rowKey + "");
        return true;
    }

    public boolean update(String[] updatedMessage) {
        if (updatedMessage.length == 0) {
            System.out.println("the input is empty");
            return false;
        }
        String columnName = updatedMessage[0];
        String value = updatedMessage[1];
        if (!coloumNameAndType.containsKey(columnName)) {
            System.out.println("the input is not valid, cannot find the column name: " + columnName);
            return false;
        }
        row.put(columnName, value);
        return true;
    }

    private boolean checkValid(String columnName, String value) {
        if (!coloumNameAndType.containsKey(columnName)) {
            System.out.println("the input is not valid, cannot find the column name: " + columnName);
            return false;
        }
        String type = coloumNameAndType.get(columnName);
        if (type.equals("Varchar")) {
            return true;
        }
        if (type.equals("Int")) {
            for (int i = 0; i < value.length(); i++) {
                if (value.charAt(i) < '0' || value.charAt(i) > '9') {
                    System.out.println("the input is not valid, the type should be int, However the input is: " + value);
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public Map<String, String> getRow() {
        return row;
    }
}
