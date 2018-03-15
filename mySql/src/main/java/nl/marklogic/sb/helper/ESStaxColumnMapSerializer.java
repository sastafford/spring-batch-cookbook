package nl.marklogic.sb.helper;

import com.marklogic.spring.batch.columnmap.ColumnMapSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

public class ESStaxColumnMapSerializer implements ColumnMapSerializer {
    private final static Logger logger = LoggerFactory.getLogger(ESStaxColumnMapSerializer.class);

    private XMLOutputFactory xmlOutputFactory;

    public ESStaxColumnMapSerializer() {
        this.xmlOutputFactory = XMLOutputFactory.newFactory();
    }

    @Override
    public String serializeColumnMap(Map<String, Object> columnMap, String rootLocalName) {
        StringWriter out = new StringWriter();
        try {
            XMLStreamWriter sw = xmlOutputFactory.createXMLStreamWriter(out);
            sw.writeStartElement(rootLocalName);
            writeColumnMap(columnMap, sw);
            sw.writeEndElement();
            sw.flush();
        } catch (XMLStreamException ex) {
            throw new RuntimeException(ex);
        }
        return out.toString();
    }

    /**
     * Recursive function that will call itself when it finds that the value of a key/value pair in the column map is a
     * Map<String, Object> itself.
     *
     * @param columnMap
     * @param sw
     * @throws XMLStreamException
     */
    private void writeColumnMap(Map<String, Object> columnMap, XMLStreamWriter sw) throws XMLStreamException {
        for (String key : columnMap.keySet()) {
            Object value = columnMap.get(key);
            String elementName = key.replace(" ","_");
            if (value != null) {
                if (value instanceof List) {
                    List list = (List) value;
                    for (Object item : list) {
                        if (item != null) {
                            sw.writeStartElement(elementName);
                            if (item instanceof Map) {
                                writeColumnMap((Map<String, Object>) item, sw);
                            } else {
                                sw.writeCharacters(item.toString());
                            }
                            sw.writeEndElement();
                        }
                    }
                } else if (value instanceof Map) {
                    sw.writeStartElement(elementName);
                    writeColumnMap((Map<String, Object>) value, sw);
                    sw.writeEndElement();
                } else if (value instanceof byte[]) {
                    // TODO Figure out what to do with blobs by default
                    logger.info("Ignoring blob, key: " + key);
                } else {
                    String text = value.toString();
                    if (text != null && text.trim().length() > 0) {
                        sw.writeStartElement(elementName);
                        sw.writeCharacters(text);
                        sw.writeEndElement();
                    }
                }
            }
        }
    }


}