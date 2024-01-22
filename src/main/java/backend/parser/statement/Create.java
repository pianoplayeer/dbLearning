package backend.parser.statement;

import java.util.HashSet;
import java.util.Set;

public class Create {
    public String tableName;
    public String[] fieldName;
    public String[] fieldType;
    public Set<String> index;
}
