package com.alibaba.datax.plugin.writer.s3writer.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.s3writer.Constant;
import com.alibaba.datax.plugin.writer.s3writer.Key;
import com.alibaba.datax.plugin.writer.s3writer.S3WriterErrorCode;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

/**
 * @author xiangying
 */
public class S3Util {
    public static AmazonS3 initS3Client(Configuration conf) {
        String endpoint = conf.getString(Key.ENDPOINT);
        String region = conf.getString(Key.REGION);
        String accessId = conf.getString(Key.ACCESSID);
        String accessKey = conf.getString(Key.ACCESSKEY);
        boolean pathStyleAccess = conf.getBool(Key.PATH_STYLE_ACCESS, true);

        ClientConfiguration s3Conf = new ClientConfiguration();
        s3Conf.setSocketTimeout(Constant.SOCKETTIMEOUT);

        AmazonS3 client = null;
        try {
            AWSCredentials credentials = new BasicAWSCredentials(accessId, accessKey);
            client = AmazonS3ClientBuilder.standard()
                    .withCredentials((new AWSStaticCredentialsProvider(credentials)))
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                    .withPathStyleAccessEnabled(pathStyleAccess)
                    .withClientConfiguration(s3Conf)
                    .build();
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    S3WriterErrorCode.ILLEGAL_VALUE, e.getMessage());
        }

        return client;
    }
}
