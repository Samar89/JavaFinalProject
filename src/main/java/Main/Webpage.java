/**
 * Copyright (c) 2014 Oracle and/or its affiliates. All rights reserved.
 *
 * You may not modify, use, reproduce, or distribute this software except in
 * compliance with  the terms of the License at:
 * https://github.com/javaee/tutorial-examples/LICENSE.txt
 */
package Main;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import java.util.*;

/**
 * Root resource (exposed at "FinalJavaProject" path)
 */
@Path("JavaWebPage")
public class Webpage {
    @Context
    private UriInfo context;

    /** Creates a new instance of HelloWorld */
    public Webpage() {
    }

    /**
     * Retrieves representation of an instance of FinalJavaProject.Webpage
     * @return an instance of java.lang.String
     */
    @GET
    @Produces("text/html")
    public String getHtml() {
        //TODO replace with your own HTML
        String HtmlCode = "";
        
        ReadCsvFileIntoSparkSql getData = new ReadCsvFileIntoSparkSql();
        
        List<Jobs> alljobs = getData.getData();
        
        for(Jobs j:alljobs) {
            HtmlCode += j.getTitle();
        }
        
        return "<html><head><title>Final Java Project</title></head><body>"+HtmlCode+"</body></html>";
    }

    /**
     * PUT method for updating or creating an instance of Webpage
     * @param content representation for the resource
     * @return an HTTP response with content of the updated or created resource.
     */
    @PUT
    @Consumes("text/html")
    public void putHtml(String content) {
    }
}
