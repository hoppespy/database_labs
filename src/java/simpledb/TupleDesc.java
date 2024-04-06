package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t){
            this.fieldType = t;
            this.fieldName = null;
        }
        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    private List<TDItem> items;
    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @paramtypeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @paramfieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(){}
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        items = new ArrayList<>();
        for(int i=0;i<typeAr.length;i++){
            TDItem item = new TDItem(typeAr[i],fieldAr[i]);
            items.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        items = new ArrayList<>();
        for(Type tp:typeAr){
            TDItem item = new TDItem(tp);
            items.add(item);
        }
        // some code goes here
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0||i>=items.size()){
            throw new NoSuchElementException("下标"+i+"超出了范围");
        }
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0||i>=items.size()){
            throw new NoSuchElementException("下标"+i+"超出了范围");
        }
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(int i=0;i< items.size();i++){
            if(items.get(i).fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException("找不到名为"+name+"的元素");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int len = 0;
        for(TDItem item:items){
            len += item.fieldType.getLen();
        }
        return len;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @paramtd1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @paramtd2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public List<TDItem> getItems() {
        return items;
    }

    public void setItems(List<TDItem> items) {
        this.items = items;
    }

    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> items = new ArrayList<>();
        items.addAll(td1.getItems());
        items.addAll(td2.getItems());
        TupleDesc tupleDesc = new TupleDesc();
        tupleDesc.setItems(items);
        return tupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if(!o.getClass().isInstance(TupleDesc.class)){
            return false;
        }
        TupleDesc tupleDesc = (TupleDesc) o;
        int len1 = this.items.size();
        List<TDItem> items = tupleDesc.getItems();
        int len2 = items.size();
        if(len1!=len2) return false;
        for(int i=0;i<len1;i++){
            TDItem item1 = this.items.get(i);
            TDItem item2 = tupleDesc.getItems().get(i);
            if(!item1.fieldType.equals(item2.fieldType)){
                return false;
            }
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder builder = new StringBuilder();
        for(int i=0;i< items.size();i++){
            builder.append(items.get(i).fieldType+"("+items.get(i).fieldName+")"+" ");
        }
        return builder.toString();
    }
}
