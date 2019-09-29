import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements TableInterface {
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private int rowKey;
    private String name;
    private Map<Integer, DbRow> tableMap;
    private Map<String, String> coloInformation;
    public Table(String tableName, String[][] coloumAndType) {
        name = tableName;
        rowKey = 1;
        tableMap = new LinkedHashMap<>();
        coloInformation = new HashMap<>();
        for (int i = 0; i < coloumAndType.length; i++) {
            coloInformation.put(coloumAndType[i][0], coloumAndType[i][1]);
        }
    }

    public boolean insertRow(String[][] newRow) {
        lock.writeLock().lock();
        try {
            DbRow curRow = new DbRow(coloInformation);
            if (!curRow.createRow(newRow, rowKey)) {
                return false;
            }
            tableMap.put(rowKey, curRow);
            rowKey++;
            return true;
        } finally {
            lock.writeLock().unlock();
        }

    }

    public boolean deleteRow(int rowKey) {
        lock.writeLock().lock();
        try {
            if (tableMap.containsKey(rowKey)) {
                tableMap.remove(rowKey);
                return true;
            }
            return false;
        } finally {
            lock.writeLock().unlock();
        }

    }

    public boolean updateRow(int rowKey, String[] updatedMessage) {
        lock.writeLock().lock();
        try {
            if (!tableMap.containsKey(rowKey)) {
                return false;
            }
            DbRow targetRow = tableMap.get(rowKey);
            targetRow.update(updatedMessage);
            return true;
        } finally {
            lock.writeLock().unlock();
        }

    }

    public Map<Integer, DbRow> getAllRows() {
        lock.readLock().lock();
        try {
            return new LinkedHashMap<>(tableMap);          // For safty, I do not want my tableMap can be visited from outside. So I copy a new tableMap and return this new Map.
        } finally {
            lock.readLock().unlock();
        }

    }

    public Map<Integer, DbRow> firstKRows(int k) {
        lock.readLock().lock();
        try {
            Map<Integer, DbRow> firstKEntry = new LinkedHashMap<>();
            int index = 0;
            for (Map.Entry<Integer, DbRow> myEntry : tableMap.entrySet()) {
                if (index >= k) {
                    break;
                }
                firstKEntry.put(myEntry.getKey(), myEntry.getValue());
                index++;
            }
            return firstKEntry;
        } finally {
            lock.readLock().unlock();
        }

    }

    public List<DbRow> sortBy(String coloumName, int num) {
        lock.readLock().lock();
        try {
            List<DbRow> sortedResult = new ArrayList<>();
            for (Map.Entry<Integer, DbRow> myEntry : tableMap.entrySet()) {
                sortedResult.add(myEntry.getValue());
            }
            Collections.sort(sortedResult, new Comparator<DbRow>() {
                @Override
                public int compare(DbRow o1, DbRow o2) {
                    if (!o1.getRow().containsKey(coloumName)) {
                        return 1;
                    }
                    if (!o2.getRow().containsKey(coloumName)) {
                        return 1;
                    }
                    String o1Value = o1.getRow().get(coloumName);
                    String o2Value = o2.getRow().get(coloumName);
                    return o1Value.compareTo(o2Value);
                }
            });
            List<DbRow> kthSortedResult = new ArrayList<>();
            for (int i = 0; i < num; i++) {
                kthSortedResult.add(sortedResult.get(i));
            }
            return kthSortedResult;
        } finally {
            lock.readLock().unlock();
        }

    }

    public List<Object[]> groupBy(String coloumName, int k) {
        lock.readLock().lock();
        try {
            Map<String, Integer> sameColunmValue = new LinkedHashMap<>();
            for (Map.Entry<Integer, DbRow> myEntry : tableMap.entrySet()) {
                if (!myEntry.getValue().getRow().containsKey(coloumName)) {
                    continue;
                }
                String curColumn = myEntry.getValue().getRow().get(coloumName);
                sameColunmValue.put(curColumn, sameColunmValue.getOrDefault(curColumn, 0) + 1);
            }
            List<Object[]> groupByList = new ArrayList<>();
            int count = 0;
            for (Map.Entry<String, Integer> entry : sameColunmValue.entrySet()) {
                if (count >= k) {
                    break;
                }
                Object[] temp = new Object[]{entry.getKey(), entry.getValue()};
                groupByList.add(temp);
                count++;
            }
            return groupByList;
        } finally {
            lock.readLock().unlock();
        }

    }

    public String getName() {
        return name;
    }

    public Map<String, String> getColoInformation() {
        return new HashMap<>(coloInformation);
    }
}
