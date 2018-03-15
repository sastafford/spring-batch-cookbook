package nl.marklogic.sb.helper;

import com.marklogic.spring.batch.item.writer.support.DefaultUriTransformer;

public class MySqlUriTransformer extends DefaultUriTransformer {

    @Override
    public String transform(String uri) {
        uri = uri.replace(" ","_").replace(":","-").replace(".","-");
        return super.transform(uri);
    }

}
