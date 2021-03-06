/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package ar.uba.fi.tppro.core.service.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParalellSearchResult implements org.apache.thrift.TBase<ParalellSearchResult, ParalellSearchResult._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ParalellSearchResult");

  private static final org.apache.thrift.protocol.TField QR_FIELD_DESC = new org.apache.thrift.protocol.TField("qr", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField ERRORS_FIELD_DESC = new org.apache.thrift.protocol.TField("errors", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ParalellSearchResultStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ParalellSearchResultTupleSchemeFactory());
  }

  public QueryResult qr; // required
  public List<Error> errors; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    QR((short)1, "qr"),
    ERRORS((short)2, "errors");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // QR
          return QR;
        case 2: // ERRORS
          return ERRORS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.QR, new org.apache.thrift.meta_data.FieldMetaData("qr", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, QueryResult.class)));
    tmpMap.put(_Fields.ERRORS, new org.apache.thrift.meta_data.FieldMetaData("errors", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Error.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ParalellSearchResult.class, metaDataMap);
  }

  public ParalellSearchResult() {
  }

  public ParalellSearchResult(
    QueryResult qr,
    List<Error> errors)
  {
    this();
    this.qr = qr;
    this.errors = errors;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ParalellSearchResult(ParalellSearchResult other) {
    if (other.isSetQr()) {
      this.qr = new QueryResult(other.qr);
    }
    if (other.isSetErrors()) {
      List<Error> __this__errors = new ArrayList<Error>();
      for (Error other_element : other.errors) {
        __this__errors.add(new Error(other_element));
      }
      this.errors = __this__errors;
    }
  }

  public ParalellSearchResult deepCopy() {
    return new ParalellSearchResult(this);
  }

  @Override
  public void clear() {
    this.qr = null;
    this.errors = null;
  }

  public QueryResult getQr() {
    return this.qr;
  }

  public ParalellSearchResult setQr(QueryResult qr) {
    this.qr = qr;
    return this;
  }

  public void unsetQr() {
    this.qr = null;
  }

  /** Returns true if field qr is set (has been assigned a value) and false otherwise */
  public boolean isSetQr() {
    return this.qr != null;
  }

  public void setQrIsSet(boolean value) {
    if (!value) {
      this.qr = null;
    }
  }

  public int getErrorsSize() {
    return (this.errors == null) ? 0 : this.errors.size();
  }

  public java.util.Iterator<Error> getErrorsIterator() {
    return (this.errors == null) ? null : this.errors.iterator();
  }

  public void addToErrors(Error elem) {
    if (this.errors == null) {
      this.errors = new ArrayList<Error>();
    }
    this.errors.add(elem);
  }

  public List<Error> getErrors() {
    return this.errors;
  }

  public ParalellSearchResult setErrors(List<Error> errors) {
    this.errors = errors;
    return this;
  }

  public void unsetErrors() {
    this.errors = null;
  }

  /** Returns true if field errors is set (has been assigned a value) and false otherwise */
  public boolean isSetErrors() {
    return this.errors != null;
  }

  public void setErrorsIsSet(boolean value) {
    if (!value) {
      this.errors = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case QR:
      if (value == null) {
        unsetQr();
      } else {
        setQr((QueryResult)value);
      }
      break;

    case ERRORS:
      if (value == null) {
        unsetErrors();
      } else {
        setErrors((List<Error>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case QR:
      return getQr();

    case ERRORS:
      return getErrors();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case QR:
      return isSetQr();
    case ERRORS:
      return isSetErrors();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ParalellSearchResult)
      return this.equals((ParalellSearchResult)that);
    return false;
  }

  public boolean equals(ParalellSearchResult that) {
    if (that == null)
      return false;

    boolean this_present_qr = true && this.isSetQr();
    boolean that_present_qr = true && that.isSetQr();
    if (this_present_qr || that_present_qr) {
      if (!(this_present_qr && that_present_qr))
        return false;
      if (!this.qr.equals(that.qr))
        return false;
    }

    boolean this_present_errors = true && this.isSetErrors();
    boolean that_present_errors = true && that.isSetErrors();
    if (this_present_errors || that_present_errors) {
      if (!(this_present_errors && that_present_errors))
        return false;
      if (!this.errors.equals(that.errors))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(ParalellSearchResult other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    ParalellSearchResult typedOther = (ParalellSearchResult)other;

    lastComparison = Boolean.valueOf(isSetQr()).compareTo(typedOther.isSetQr());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQr()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.qr, typedOther.qr);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetErrors()).compareTo(typedOther.isSetErrors());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetErrors()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.errors, typedOther.errors);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ParalellSearchResult(");
    boolean first = true;

    sb.append("qr:");
    if (this.qr == null) {
      sb.append("null");
    } else {
      sb.append(this.qr);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("errors:");
    if (this.errors == null) {
      sb.append("null");
    } else {
      sb.append(this.errors);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (qr != null) {
      qr.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ParalellSearchResultStandardSchemeFactory implements SchemeFactory {
    public ParalellSearchResultStandardScheme getScheme() {
      return new ParalellSearchResultStandardScheme();
    }
  }

  private static class ParalellSearchResultStandardScheme extends StandardScheme<ParalellSearchResult> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ParalellSearchResult struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // QR
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.qr = new QueryResult();
              struct.qr.read(iprot);
              struct.setQrIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // ERRORS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list26 = iprot.readListBegin();
                struct.errors = new ArrayList<Error>(_list26.size);
                for (int _i27 = 0; _i27 < _list26.size; ++_i27)
                {
                  Error _elem28; // required
                  _elem28 = new Error();
                  _elem28.read(iprot);
                  struct.errors.add(_elem28);
                }
                iprot.readListEnd();
              }
              struct.setErrorsIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ParalellSearchResult struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.qr != null) {
        oprot.writeFieldBegin(QR_FIELD_DESC);
        struct.qr.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.errors != null) {
        oprot.writeFieldBegin(ERRORS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.errors.size()));
          for (Error _iter29 : struct.errors)
          {
            _iter29.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ParalellSearchResultTupleSchemeFactory implements SchemeFactory {
    public ParalellSearchResultTupleScheme getScheme() {
      return new ParalellSearchResultTupleScheme();
    }
  }

  private static class ParalellSearchResultTupleScheme extends TupleScheme<ParalellSearchResult> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ParalellSearchResult struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetQr()) {
        optionals.set(0);
      }
      if (struct.isSetErrors()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetQr()) {
        struct.qr.write(oprot);
      }
      if (struct.isSetErrors()) {
        {
          oprot.writeI32(struct.errors.size());
          for (Error _iter30 : struct.errors)
          {
            _iter30.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ParalellSearchResult struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.qr = new QueryResult();
        struct.qr.read(iprot);
        struct.setQrIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list31 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.errors = new ArrayList<Error>(_list31.size);
          for (int _i32 = 0; _i32 < _list31.size; ++_i32)
          {
            Error _elem33; // required
            _elem33 = new Error();
            _elem33.read(iprot);
            struct.errors.add(_elem33);
          }
        }
        struct.setErrorsIsSet(true);
      }
    }
  }

}

