package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */


    private final Catalog catalog;

    private int tableId;
    private final TransactionId tid;
    private TupleDesc tupleDesc;
    private String tableName;
    private String tableAlias;
    private DbFileIterator iterator;
    private DbFile file;

    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.catalog = Database.getCatalog();

        this.tid = tid;
        this.tableId = tableid;
        this.tupleDesc = changeTupleDesc(catalog.getTupleDesc(tableid), tableAlias);

        this.tableName = catalog.getTableName(tableid);
        this.tableAlias = tableAlias;

        this.file = catalog.getDatabaseFile(tableid);


    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return this.tableName;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        this.tupleDesc = changeTupleDesc(catalog.getTupleDesc(tableid), tableAlias);
        this.tableName = catalog.getTableName(tableid);
        this.file = catalog.getDatabaseFile(tableid);
        try {
            open();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

    }

    public SeqScan(TransactionId tid, int tableid) {

        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.iterator = this.file.iterator(this.tid);
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (iterator == null) {
            return false;
        }
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (iterator == null) {
            throw new NoSuchElementException("No next tuple");
        }
        Tuple tuple = iterator.next();
        if (tuple == null) {
            throw new NoSuchElementException("No next tuple");
        }
        return tuple;
    }

    public void close() {
        // some code goes here
        iterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        iterator.rewind();
    }
    private TupleDesc changeTupleDesc(TupleDesc desc, String alias) {
        TupleDesc res = new TupleDesc();
        List<TupleDesc.TDItem> items = new ArrayList<>();
        List<TupleDesc.TDItem> tdItems = desc.getItems();
        for (TupleDesc.TDItem tdItem : tdItems) {
            TupleDesc.TDItem item = new TupleDesc.TDItem(tdItem.fieldType, alias + "." +tdItem.fieldName);
            items.add(item);
        }
        res.setItems(items);
        return res;
    }
}
