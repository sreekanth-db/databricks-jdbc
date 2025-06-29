/**
 * Autogenerated by Thrift Compiler (0.19.0)
 *
 * <p>DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 * @generated
 */
package com.databricks.jdbc.model.client.thrift.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(
    value = "Autogenerated by Thrift Compiler (0.19.0)",
    date = "2025-05-08")
public class TMapTypeEntry
    implements org.apache.thrift.TBase<TMapTypeEntry, TMapTypeEntry._Fields>,
        java.io.Serializable,
        Cloneable,
        Comparable<TMapTypeEntry> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC =
      new org.apache.thrift.protocol.TStruct("TMapTypeEntry");

  private static final org.apache.thrift.protocol.TField KEY_TYPE_PTR_FIELD_DESC =
      new org.apache.thrift.protocol.TField(
          "keyTypePtr", org.apache.thrift.protocol.TType.I32, (short) 1);
  private static final org.apache.thrift.protocol.TField VALUE_TYPE_PTR_FIELD_DESC =
      new org.apache.thrift.protocol.TField(
          "valueTypePtr", org.apache.thrift.protocol.TType.I32, (short) 2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY =
      new TMapTypeEntryStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY =
      new TMapTypeEntryTupleSchemeFactory();

  public int keyTypePtr; // required
  public int valueTypePtr; // required

  /**
   * The set of fields this struct contains, along with convenience methods for finding and
   * manipulating them.
   */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    KEY_TYPE_PTR((short) 1, "keyTypePtr"),
    VALUE_TYPE_PTR((short) 2, "valueTypePtr");

    private static final java.util.Map<java.lang.String, _Fields> byName =
        new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /** Find the _Fields constant that matches fieldId, or null if its not found. */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
        case 1: // KEY_TYPE_PTR
          return KEY_TYPE_PTR;
        case 2: // VALUE_TYPE_PTR
          return VALUE_TYPE_PTR;
        default:
          return null;
      }
    }

    /** Find the _Fields constant that matches fieldId, throwing an exception if it is not found. */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null)
        throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /** Find the _Fields constant that matches name, or null if its not found. */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __KEYTYPEPTR_ISSET_ID = 0;
  private static final int __VALUETYPEPTR_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;

  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap =
        new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(
        _Fields.KEY_TYPE_PTR,
        new org.apache.thrift.meta_data.FieldMetaData(
            "keyTypePtr",
            org.apache.thrift.TFieldRequirementType.REQUIRED,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32, "TTypeEntryPtr")));
    tmpMap.put(
        _Fields.VALUE_TYPE_PTR,
        new org.apache.thrift.meta_data.FieldMetaData(
            "valueTypePtr",
            org.apache.thrift.TFieldRequirementType.REQUIRED,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32, "TTypeEntryPtr")));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        TMapTypeEntry.class, metaDataMap);
  }

  public TMapTypeEntry() {}

  public TMapTypeEntry(int keyTypePtr, int valueTypePtr) {
    this();
    this.keyTypePtr = keyTypePtr;
    setKeyTypePtrIsSet(true);
    this.valueTypePtr = valueTypePtr;
    setValueTypePtrIsSet(true);
  }

  /** Performs a deep copy on <i>other</i>. */
  public TMapTypeEntry(TMapTypeEntry other) {
    __isset_bitfield = other.__isset_bitfield;
    this.keyTypePtr = other.keyTypePtr;
    this.valueTypePtr = other.valueTypePtr;
  }

  @Override
  public TMapTypeEntry deepCopy() {
    return new TMapTypeEntry(this);
  }

  @Override
  public void clear() {
    setKeyTypePtrIsSet(false);
    this.keyTypePtr = 0;
    setValueTypePtrIsSet(false);
    this.valueTypePtr = 0;
  }

  public int getKeyTypePtr() {
    return this.keyTypePtr;
  }

  public TMapTypeEntry setKeyTypePtr(int keyTypePtr) {
    this.keyTypePtr = keyTypePtr;
    setKeyTypePtrIsSet(true);
    return this;
  }

  public void unsetKeyTypePtr() {
    __isset_bitfield =
        org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __KEYTYPEPTR_ISSET_ID);
  }

  /** Returns true if field keyTypePtr is set (has been assigned a value) and false otherwise */
  public boolean isSetKeyTypePtr() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __KEYTYPEPTR_ISSET_ID);
  }

  public void setKeyTypePtrIsSet(boolean value) {
    __isset_bitfield =
        org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __KEYTYPEPTR_ISSET_ID, value);
  }

  public int getValueTypePtr() {
    return this.valueTypePtr;
  }

  public TMapTypeEntry setValueTypePtr(int valueTypePtr) {
    this.valueTypePtr = valueTypePtr;
    setValueTypePtrIsSet(true);
    return this;
  }

  public void unsetValueTypePtr() {
    __isset_bitfield =
        org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __VALUETYPEPTR_ISSET_ID);
  }

  /** Returns true if field valueTypePtr is set (has been assigned a value) and false otherwise */
  public boolean isSetValueTypePtr() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __VALUETYPEPTR_ISSET_ID);
  }

  public void setValueTypePtrIsSet(boolean value) {
    __isset_bitfield =
        org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __VALUETYPEPTR_ISSET_ID, value);
  }

  @Override
  public void setFieldValue(
      _Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
      case KEY_TYPE_PTR:
        if (value == null) {
          unsetKeyTypePtr();
        } else {
          setKeyTypePtr((java.lang.Integer) value);
        }
        break;

      case VALUE_TYPE_PTR:
        if (value == null) {
          unsetValueTypePtr();
        } else {
          setValueTypePtr((java.lang.Integer) value);
        }
        break;
    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
      case KEY_TYPE_PTR:
        return getKeyTypePtr();

      case VALUE_TYPE_PTR:
        return getValueTypePtr();
    }
    throw new java.lang.IllegalStateException();
  }

  /**
   * Returns true if field corresponding to fieldID is set (has been assigned a value) and false
   * otherwise
   */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
      case KEY_TYPE_PTR:
        return isSetKeyTypePtr();
      case VALUE_TYPE_PTR:
        return isSetValueTypePtr();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TMapTypeEntry) return this.equals((TMapTypeEntry) that);
    return false;
  }

  public boolean equals(TMapTypeEntry that) {
    if (that == null) return false;
    if (this == that) return true;

    boolean this_present_keyTypePtr = true;
    boolean that_present_keyTypePtr = true;
    if (this_present_keyTypePtr || that_present_keyTypePtr) {
      if (!(this_present_keyTypePtr && that_present_keyTypePtr)) return false;
      if (this.keyTypePtr != that.keyTypePtr) return false;
    }

    boolean this_present_valueTypePtr = true;
    boolean that_present_valueTypePtr = true;
    if (this_present_valueTypePtr || that_present_valueTypePtr) {
      if (!(this_present_valueTypePtr && that_present_valueTypePtr)) return false;
      if (this.valueTypePtr != that.valueTypePtr) return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + keyTypePtr;

    hashCode = hashCode * 8191 + valueTypePtr;

    return hashCode;
  }

  @Override
  public int compareTo(TMapTypeEntry other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetKeyTypePtr(), other.isSetKeyTypePtr());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetKeyTypePtr()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.keyTypePtr, other.keyTypePtr);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetValueTypePtr(), other.isSetValueTypePtr());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValueTypePtr()) {
      lastComparison =
          org.apache.thrift.TBaseHelper.compareTo(this.valueTypePtr, other.valueTypePtr);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  @Override
  public void write(org.apache.thrift.protocol.TProtocol oprot)
      throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TMapTypeEntry(");
    boolean first = true;

    sb.append("keyTypePtr:");
    sb.append(this.keyTypePtr);
    first = false;
    if (!first) sb.append(", ");
    sb.append("valueTypePtr:");
    sb.append(this.valueTypePtr);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'keyTypePtr' because it's a primitive and you chose the non-beans
    // generator.
    // alas, we cannot check 'valueTypePtr' because it's a primitive and you chose the non-beans
    // generator.
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(
          new org.apache.thrift.protocol.TCompactProtocol(
              new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in)
      throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and
      // doesn't call the default constructor.
      __isset_bitfield = 0;
      read(
          new org.apache.thrift.protocol.TCompactProtocol(
              new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TMapTypeEntryStandardSchemeFactory
      implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TMapTypeEntryStandardScheme getScheme() {
      return new TMapTypeEntryStandardScheme();
    }
  }

  private static class TMapTypeEntryStandardScheme
      extends org.apache.thrift.scheme.StandardScheme<TMapTypeEntry> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, TMapTypeEntry struct)
        throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true) {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
          break;
        }
        switch (schemeField.id) {
          case 1: // KEY_TYPE_PTR
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.keyTypePtr = iprot.readI32();
              struct.setKeyTypePtrIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // VALUE_TYPE_PTR
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.valueTypePtr = iprot.readI32();
              struct.setValueTypePtrIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetKeyTypePtr()) {
        throw new org.apache.thrift.protocol.TProtocolException(
            "Required field 'keyTypePtr' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetValueTypePtr()) {
        throw new org.apache.thrift.protocol.TProtocolException(
            "Required field 'valueTypePtr' was not found in serialized data! Struct: "
                + toString());
      }
      struct.validate();
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot, TMapTypeEntry struct)
        throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(KEY_TYPE_PTR_FIELD_DESC);
      oprot.writeI32(struct.keyTypePtr);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(VALUE_TYPE_PTR_FIELD_DESC);
      oprot.writeI32(struct.valueTypePtr);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }
  }

  private static class TMapTypeEntryTupleSchemeFactory
      implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TMapTypeEntryTupleScheme getScheme() {
      return new TMapTypeEntryTupleScheme();
    }
  }

  private static class TMapTypeEntryTupleScheme
      extends org.apache.thrift.scheme.TupleScheme<TMapTypeEntry> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TMapTypeEntry struct)
        throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot =
          (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI32(struct.keyTypePtr);
      oprot.writeI32(struct.valueTypePtr);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TMapTypeEntry struct)
        throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot =
          (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.keyTypePtr = iprot.readI32();
      struct.setKeyTypePtrIsSet(true);
      struct.valueTypePtr = iprot.readI32();
      struct.setValueTypePtrIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(
      org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme())
            ? STANDARD_SCHEME_FACTORY
            : TUPLE_SCHEME_FACTORY)
        .getScheme();
  }
}
