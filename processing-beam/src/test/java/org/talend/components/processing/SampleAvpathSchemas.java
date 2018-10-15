package org.talend.components.processing;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;
import org.talend.daikon.avro.AvroUtils;

import java.util.*;

/**
 * Sample Avro data to test avpath.
 */
public class SampleAvpathSchemas {

    /**
     * Tools to create IndexedRecords that match the following pseudo-avro:
     * <p>
     * 
     * <pre>
     * {
     *   "automobiles" : [
     *     { "maker" : "Nissan", "model" : "Teana", "year" : 2011 },
     *     { "maker" : "Honda", "model" : "Jazz", "year" : 2010 },
     *     { "maker" : "Honda", "model" : "Civic", "year" : 2007 },
     *     { "maker" : "Toyota", "model" : "Yaris", "year" : 2008 },
     *     { "maker" :* "Honda", "model" : "Accord", "year" : 2011 }
     *   ],
     *   "motorcycles" : [{ "maker" : "Honda", "model" : "ST1300", "year" : 2012 }]
     * }
     * </pre>
     */
    public static class Vehicles {

        public final static Schema RECORD_VEHICLE = SchemaBuilder.record("vehicle").fields() //
                .requiredString("maker") //
                .requiredString("model") //
                .requiredInt("year") //
                .endRecord();

        public final static Schema RECORD_VEHICLE_COLLECTION = SchemaBuilder.record("vehicleCollection").fields() //
                .name("automobiles").type().array().items().type(RECORD_VEHICLE).noDefault() //
                .name("motorcycles").type().array().items().type(RECORD_VEHICLE).noDefault() //
                .endRecord();

        public static final IndexedRecord createVehicleRecord(String maker, String model, int year) {
            IndexedRecord ir = new GenericData.Record(RECORD_VEHICLE);
            ir.put(0, maker);
            ir.put(1, model);
            ir.put(2, year);
            return ir;
        }

        public static final IndexedRecord createVehicleCollection(Collection<IndexedRecord> automobiles,
                Collection<IndexedRecord> motorcycles) {
            IndexedRecord r = new Record(RECORD_VEHICLE_COLLECTION);
            r.put(0, new GenericData.Array<IndexedRecord>(RECORD_VEHICLE_COLLECTION.getFields().get(0).schema(), automobiles));
            r.put(1, new GenericData.Array<IndexedRecord>(RECORD_VEHICLE_COLLECTION.getFields().get(1).schema(), motorcycles));
            return r;
        }

        public static final IndexedRecord getDefaultVehicleCollection() {
            return createVehicleCollection(Arrays.asList(createVehicleRecord("Nissan", "Teana", 2011), //
                    createVehicleRecord("Honda", "Jazz", 2010), //
                    createVehicleRecord("Honda", "Civic", 2007), //
                    createVehicleRecord("Toyota", "Yaris", 2017), //
                    createVehicleRecord("Toyota", "Yaris", 2016), //
                    createVehicleRecord("Honda", "Accord", 2011)), //
                    Arrays.asList(createVehicleRecord("Honda", "ST1300", 2012)));
        }
    }

    /**
     * Tools to create IndexedRecords that match the following pseudo-avro:
     *
     * <pre>
     * {
     *     "books" : [
     *     {
     *         "id"     : 1,
     *             "title"  : "Clean Code",
     *             "author" : { "name" : "Robert C. Martin" },
     *         "price"  : 17.96
     *     },
     *    {
     *        "id"     : 2,
     *            "title"  : "Maintainable JavaScript",
     *            "author" : { "name" : "Nicholas C. Zakas" },
     *        "price"  : 10
     *    },
     *    {
     *        "id"     : 3,
     *            "title"  : "Agile Software Development",
     *            "author" : { "name" : "Robert C. Martin" },
     *        "price"  : 20
     *    },
     *    {
     *        "id"     : 4,
     *            "title"  : "JavaScript: The Good Parts",
     *            "author" : { "name" : "Douglas Crockford" },
     *        "price"  : 15.67
     *    }
     *    ]
     *}
     * </pre>
     */
    public static class BookCollection {

        public final static Schema RECORD_BOOK = SchemaBuilder.record("book").fields() //
                .requiredInt("id") //
                .requiredString("title") //
                .requiredString("author") //
                .requiredFloat("price") //
                .endRecord();

        public final static Schema RECORD_BOOK_COLLECTION = SchemaBuilder.record("books").fields() //
                .name("books").type().array().items().type(RECORD_BOOK).noDefault() //
                .endRecord();

        public static final IndexedRecord createBook(int id, String title, String author, float price) {
            IndexedRecord ir = new GenericData.Record(RECORD_BOOK);
            ir.put(0, id);
            ir.put(1, title);
            ir.put(2, author);
            ir.put(3, price);
            return ir;
        }

        public static final IndexedRecord createBookCollection(IndexedRecord... books) {
            IndexedRecord r = new Record(RECORD_BOOK_COLLECTION);
            r.put(0, new GenericData.Array<IndexedRecord>(RECORD_BOOK_COLLECTION.getFields().get(0).schema(),
                    Arrays.asList(books)));
            return r;
        }

        public static final IndexedRecord getDefaultBooksCollection() {
            return createBookCollection(createBook(1, "Clean Code", "Robert C. Martin", 17.96f), //
                    createBook(2, "Maintainable JavaScript", "Nicholas C. Zakas", 10.0f),
                    createBook(3, "Agile Software Development", "Robert C. Martin", 20.0f),
                    createBook(4, "JavaScript: The Good Parts", "Douglas Crockford", 15.67f));
        }
    }

    /**
     * Tools to create synthetic datasets.
     *
     * Each data set has records nested two levels deep, so there is the main record, a subrecord and a subsubrecord.
     *
     * Every record always has the id, name and value fields.
     *
     * For example, Dataset A has the following pseudo avro:
     *
     * <pre>
     * {
     *     "id" : 1                   // from 1 to 10
     *     "name" : "ABCDE12345",     // 10 char
     *     "value" : 0.12345,         // average 0.0,
     *     "a1" : {                   // subrecord a1
     *         "id" : 1
     *         "name" : "ABCDE12345",
     *         "value" : 0.12345,
     *         "a2" : {               // subsubrecord a2
     *             "id" : 1
     *             "name" : "ABCDE12345",
     *             "value" : 0.12345,
     *         }
     *     }
     * }
     * </pre>
     *
     * Dataset B: Identical to A with b1 and b2 replacing a1 and a2 for the nested records, and where b1 is an array of
     * records (with one to three records inside).
     *
     * Dataset C: Identical to A with c1 and c2 replacing a1 and a2 for the nested records, and where c1 and c2 are arrays
     * of records.
     */
    public static class SyntheticDatasets {

        public final static String STRING_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        public final static Schema RECORD_A2 = getDefaultRecord("a2").endRecord();

        public final static Schema RECORD_A1 = getDefaultRecord("a1") //
                .name("a2").type(RECORD_A2).noDefault().endRecord();

        public final static Schema RECORD_A = getDefaultRecord("a") //
                .name("a1").type(RECORD_A1).noDefault().endRecord();

        public final static Schema RECORD_B2 = getDefaultRecord("b2").endRecord();

        public final static Schema RECORD_B1 = getDefaultRecord("b1") //
                .name("b2").type(RECORD_B2).noDefault().endRecord();

        public final static Schema RECORD_B = getDefaultRecord("b") //
                .name("b1").type().array().items().type(RECORD_B1).noDefault().endRecord();

        public final static Schema RECORD_C2 = getDefaultRecord("c2").endRecord();

        public final static Schema RECORD_C1 = getDefaultRecord("c1") //
                .name("c2").type().array().items().type(RECORD_C2).noDefault().endRecord();

        public final static Schema RECORD_C = getDefaultRecord("b") //
                .name("c1").type().array().items().type(RECORD_C1).noDefault().endRecord();

        /**
         * Starts a builder for the default record with the given name.
         */
        private static SchemaBuilder.FieldAssembler<Schema> getDefaultRecord(String name) {
            return SchemaBuilder.record(name).fields().requiredInt("id").requiredString("name").requiredDouble("value");

        }

        /**
         * Generate a random record based on the schema.
         */
        public static final IndexedRecord getRandomRecord(Random rnd, Schema schema) {
            // TODO: RowGeneratorIO should do this :/
            IndexedRecord r = new GenericData.Record(schema);
            for (Schema.Field f : schema.getFields()) {
                if (AvroUtils.unwrapIfNullable(f.schema()).getType().equals(Schema.Type.INT)) {
                    r.put(f.pos(), rnd.nextInt(10) + 1);
                } else if (AvroUtils.unwrapIfNullable(f.schema()).getType().equals(Schema.Type.STRING)) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 10; i++)
                        sb.append(STRING_CHARS.charAt(rnd.nextInt(STRING_CHARS.length())));
                    r.put(f.pos(), sb.toString());
                } else if (AvroUtils.unwrapIfNullable(f.schema()).getType().equals(Schema.Type.DOUBLE)) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 10; i++)
                        sb.append(STRING_CHARS.charAt(rnd.nextInt(STRING_CHARS.length())));
                    r.put(f.pos(), rnd.nextGaussian() * 10);
                } else if (AvroUtils.unwrapIfNullable(f.schema()).getType().equals(Schema.Type.RECORD)) {
                    r.put(f.pos(), getRandomRecord(rnd, f.schema()));
                } else if (AvroUtils.unwrapIfNullable(f.schema()).getType().equals(Schema.Type.ARRAY)) {
                    int size = rnd.nextInt(3) + 1;
                    GenericData.Array<IndexedRecord> a = new GenericData.Array<IndexedRecord>(size, f.schema());
                    for (int i = 0; i < size; i++) {
                        a.add(getRandomRecord(rnd, f.schema().getElementType()));
                    }
                    r.put(f.pos(), a);
                }
            }
            return r;
        }

        /**
         * Generate a collection of records based on the schema.
         */
        public static final IndexedRecord[] getRandomRecords(int size, Random rnd, Schema schema) {
            IndexedRecord[] result = new IndexedRecord[size];
            for (int i = 0; i < size; i++)
                result[i] = getRandomRecord(rnd, schema);
            return result;
        }

        /**
         * Create a copy of the input record set, modifying one of the input elements.
         *
         * The modified input record will have all of its top-level ARRAY fields changed to either NULL or empty. All other
         * fields and records will be unmodified. The original input collection is unmodified.
         * 
         * @param input The collection to clone.
         * @param n The element to modify
         * @param setToNull True to set the array to null, false to set it to an empty array.
         * @return a Copy of the input array with one record changed.
         */
        public static final IndexedRecord[] copyAndReplaceSubrecordArray(IndexedRecord[] input, int n, boolean setToNull) {
            IndexedRecord[] result = new IndexedRecord[input.length];
            for (int i = 0; i < input.length; i++) {
                if (n != i) {
                    result[i] = input[i];
                } else {
                    result[i] = GenericData.get().deepCopy(input[i].getSchema(), input[i]);
                    for (Schema.Field f : input[i].getSchema().getFields()) {
                        if (AvroUtils.unwrapIfNullable(f.schema()).getType().equals(Schema.Type.ARRAY)) {
                            result[i].put(f.pos(),
                                    setToNull ? null : new GenericData.Array<IndexedRecord>(0, f.schema().getElementType()));
                        }
                    }
                }
            }
            return result;
        }

        /**
         * @param main A main record.
         * @return A list containing all of the subrecords that it contains, whether it is nested in an array or not.
         */
        public static final List<IndexedRecord> getSubrecords(IndexedRecord main) {
            ArrayList<IndexedRecord> result = new ArrayList<>();
            for (Schema.Field f : main.getSchema().getFields()) {
                if (AvroUtils.unwrapIfNullable(f.schema()).getType().equals(Schema.Type.ARRAY)) {
                    for (IndexedRecord r : (Iterable<IndexedRecord>) main.get(f.pos())) {
                        result.add(r);
                    }
                } else if (AvroUtils.unwrapIfNullable(f.schema()).getType().equals(Schema.Type.RECORD)) {
                    result.add((IndexedRecord) main.get(f.pos()));
                }
            }
            return result;
        }
    }

    /**
     * Same as SyntheticDatasets, but with nullable data
     */
    public static class NullableSyntheticDatasets {

        public final static String STRING_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        public final static Schema RECORD_A2 = getDefaultRecord("a2").endRecord();

        public final static Schema RECORD_A1 = getDefaultRecord("a1") //
                .name("a2").type(RECORD_A2).noDefault().endRecord();

        public final static Schema RECORD_A = getDefaultRecord("a") //
                .name("a1").type(RECORD_A1).noDefault().endRecord();

        public final static Schema RECORD_B2 = getDefaultRecord("b2").endRecord();

        public final static Schema RECORD_B1 = getDefaultRecord("b1") //
                .name("b2").type(RECORD_B2).noDefault().endRecord();

        public final static Schema RECORD_B = getDefaultRecord("b") //
                .name("b1").type().array().items().type(RECORD_B1).noDefault().endRecord();

        public final static Schema RECORD_C2 = getDefaultRecord("c2").endRecord();

        public final static Schema RECORD_C1 = getDefaultRecord("c1") //
                .name("c2").type().array().items().type(RECORD_C2).noDefault().endRecord();

        public final static Schema RECORD_C = getDefaultRecord("b") //
                .name("c1").type().array().items().type(RECORD_C1).noDefault().endRecord();

        /**
         * Starts a builder for the default record with the given name.
         */
        private static SchemaBuilder.FieldAssembler<Schema> getDefaultRecord(String name) {
            return SchemaBuilder.record(name).fields().optionalInt("id").optionalString("name").optionalDouble("value");
        }
    }
}