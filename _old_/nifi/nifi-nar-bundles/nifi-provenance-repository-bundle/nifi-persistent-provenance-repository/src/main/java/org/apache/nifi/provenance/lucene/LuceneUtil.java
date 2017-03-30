/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.provenance.lucene;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.search.SearchTerm;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.Version;

public class LuceneUtil {

    public static final Version LUCENE_VERSION = Version.LATEST;

    public static String substringBefore(final String value, final String searchValue) {
        final int index = value.indexOf(searchValue);
        return (index < 0) ? value : value.substring(0, index);
    }

    public static String substringAfter(final String value, final String searchValue) {
        final int index = value.indexOf(searchValue);
        return (index < 0) ? value : (index > value.length() - 2) ? "" : value.substring(index + 1);
    }

    public static String substringBeforeLast(final String value, final String searchValue) {
        final int index = value.lastIndexOf(searchValue);
        return (index < 0) ? value : value.substring(0, index);
    }

    public static String substringAfterLast(final String value, final String searchValue) {
        final int index = value.lastIndexOf(searchValue);
        return (index < 0 || index >= value.length()) ? value : value.substring(index + 1);
    }

    public static File getProvenanceLogFile(final String baseName, final Collection<Path> allProvenanceLogs) {
        final List<File> logFiles = getProvenanceLogFiles(baseName, allProvenanceLogs);
        if (logFiles.size() != 1) {
            return null;
        }

        return logFiles.get(0);
    }

    public static List<File> getProvenanceLogFiles(final String baseName, final Collection<Path> allProvenanceLogs) {
        final List<File> matchingFiles = new ArrayList<>();

        final String searchString = baseName + ".";
        for (final Path path : allProvenanceLogs) {
            if (path.toFile().getName().startsWith(searchString)) {
                final File file = path.toFile();
                if ( file.exists() ) {
                    matchingFiles.add(file);
                } else {
                    final File dir = file.getParentFile();
                    final File gzFile = new File(dir, file.getName() + ".gz");
                    if ( gzFile.exists() ) {
                        matchingFiles.add(gzFile);
                    }
                }
            }
        }

        return matchingFiles;
    }

    public static org.apache.lucene.search.Query convertQuery(final org.apache.nifi.provenance.search.Query query) {
        if (query.getStartDate() == null && query.getEndDate() == null && query.getSearchTerms().isEmpty()) {
            return new MatchAllDocsQuery();
        }

        final BooleanQuery luceneQuery = new BooleanQuery();
        for (final SearchTerm searchTerm : query.getSearchTerms()) {
            final String searchValue = searchTerm.getValue();
            if (searchValue == null) {
                throw new IllegalArgumentException("Empty search value not allowed (for term '" + searchTerm.getSearchableField().getFriendlyName() + "')");
            }

            if (searchValue.contains("*") || searchValue.contains("?")) {
                luceneQuery.add(new BooleanClause(new WildcardQuery(new Term(searchTerm.getSearchableField().getSearchableFieldName(), searchTerm.getValue().toLowerCase())), Occur.MUST));
            } else {
                luceneQuery.add(new BooleanClause(new TermQuery(new Term(searchTerm.getSearchableField().getSearchableFieldName(), searchTerm.getValue().toLowerCase())), Occur.MUST));
            }
        }

        final Long minBytes = query.getMinFileSize() == null ? null : DataUnit.parseDataSize(query.getMinFileSize(), DataUnit.B).longValue();
        final Long maxBytes = query.getMaxFileSize() == null ? null : DataUnit.parseDataSize(query.getMaxFileSize(), DataUnit.B).longValue();
        if (minBytes != null || maxBytes != null) {
            luceneQuery.add(NumericRangeQuery.newLongRange(SearchableFields.FileSize.getSearchableFieldName(), minBytes, maxBytes, true, true), Occur.MUST);
        }

        final Long minDateTime = query.getStartDate() == null ? null : query.getStartDate().getTime();
        final Long maxDateTime = query.getEndDate() == null ? null : query.getEndDate().getTime();
        if (maxDateTime != null || minDateTime != null) {
            luceneQuery.add(NumericRangeQuery.newLongRange(SearchableFields.EventTime.getSearchableFieldName(), minDateTime, maxDateTime, true, true), Occur.MUST);
        }

        return luceneQuery;
    }

    /**
     * Will sort documents by filename and then file offset so that we can
     * retrieve the records efficiently
     *
     * @param documents
     *            list of {@link Document}s
     */
    public static void sortDocsForRetrieval(final List<Document> documents) {
        Collections.sort(documents, new Comparator<Document>() {
            @Override
            public int compare(final Document o1, final Document o2) {
                final String filename1 = o1.get(FieldNames.STORAGE_FILENAME);
                final String filename2 = o2.get(FieldNames.STORAGE_FILENAME);

                final int filenameComp = filename1.compareTo(filename2);
                if (filenameComp != 0) {
                    return filenameComp;
                }

                final IndexableField fileOffset1 = o1.getField(FieldNames.BLOCK_INDEX);
                final IndexableField fileOffset2 = o1.getField(FieldNames.BLOCK_INDEX);
                if ( fileOffset1 != null && fileOffset2 != null ) {
                    final int blockIndexResult = Long.compare(fileOffset1.numericValue().longValue(), fileOffset2.numericValue().longValue());
                    if ( blockIndexResult != 0 ) {
                        return blockIndexResult;
                    }

                    final long eventId1 = o1.getField(SearchableFields.Identifier.getSearchableFieldName()).numericValue().longValue();
                    final long eventId2 = o2.getField(SearchableFields.Identifier.getSearchableFieldName()).numericValue().longValue();
                    return Long.compare(eventId1, eventId2);
                }

                final long offset1 = o1.getField(FieldNames.STORAGE_FILE_OFFSET).numericValue().longValue();
                final long offset2 = o2.getField(FieldNames.STORAGE_FILE_OFFSET).numericValue().longValue();
                return Long.compare(offset1, offset2);
            }
        });
    }

    /**
     * Will group documents based on the {@link FieldNames#STORAGE_FILENAME}.
     *
     * @param documents
     *            list of {@link Document}s which will be sorted via
     *            {@link #sortDocsForRetrieval(List)} for more efficient record
     *            retrieval.
     * @return a {@link Map} of document groups with
     *         {@link FieldNames#STORAGE_FILENAME} as key and {@link List} of
     *         {@link Document}s as value.
     */
    public static Map<String, List<Document>> groupDocsByStorageFileName(final List<Document> documents) {
        Map<String, List<Document>> documentGroups = new HashMap<>();
        for (Document document : documents) {
            String fileName = document.get(FieldNames.STORAGE_FILENAME);
            if (!documentGroups.containsKey(fileName)) {
                documentGroups.put(fileName, new ArrayList<Document>());
            }
            documentGroups.get(fileName).add(document);
        }
        for (List<Document> groupedDocuments : documentGroups.values()) {
            sortDocsForRetrieval(groupedDocuments);
        }
        return documentGroups;
    }

    /**
     * Truncate a single field so that it does not exceed Lucene's byte size limit on indexed terms.
     *
     * @param field the string to be indexed
     * @return a string that can be indexed which is within Lucene's byte size limit, or null if anything goes wrong
     */
    public static String truncateIndexField(String field) {
        if (field == null) {
            return field;
        }

        Charset charset = Charset.defaultCharset();
        byte[] bytes = field.getBytes(charset);
        if (bytes.length <= IndexWriter.MAX_TERM_LENGTH) {
            return field;
        }

        // chop the field to maximum allowed byte length
        ByteBuffer bbuf = ByteBuffer.wrap(bytes, 0, IndexWriter.MAX_TERM_LENGTH);

        try {
            // decode the chopped byte buffer back into original charset
            CharsetDecoder decoder = charset.newDecoder();
            decoder.onMalformedInput(CodingErrorAction.IGNORE);
            decoder.reset();
            CharBuffer cbuf = decoder.decode(bbuf);
            return cbuf.toString();
        } catch (CharacterCodingException shouldNotHappen) {}

        // if we get here, something bad has happened
        return null;
    }
}
