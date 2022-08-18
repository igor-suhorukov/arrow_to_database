package com.github.isuhorukov.arrow.jdbc.bridge;

import com.beust.jcommander.JCommander;
import com.github.isuhorukov.arrow.jdbc.DatasetReader;
import com.github.isuhorukov.arrow.jdbc.bridge.model.CliParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowToDbCli {

    private final static Logger LOGGER = LoggerFactory.getLogger(ArrowToDbCli.class);

    public static void main(String[] args) throws Exception{
        CliParameters parameters = new CliParameters();
        JCommander jc = JCommander.newBuilder().addObject(parameters).build();
        try {
            jc.parse(args);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            jc.usage();
            return;
        }
        if(parameters.isHelp()){
            jc.usage();
            return;
        }
        long importStart = System.currentTimeMillis();
        DatasetReader.copyArrowDatasetIntoTable(parameters.getDatasetUri(), parameters.getFileFormatString(),
                parameters.getBatchSize(), parameters.getDatabaseDialect(), parameters.getJdbcDriverClass(),
                parameters.getTableName(),
                parameters.getJdbcUrl(), parameters.getUser(), parameters.getPassword(),
                parameters.getCreateTable(), parameters.getInsertSqlQuery());
        LOGGER.info("Import time: {}", (System.currentTimeMillis()-importStart));
    }
}
