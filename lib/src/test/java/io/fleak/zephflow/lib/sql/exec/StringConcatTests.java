/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.sql.exec;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.sql.SQLInterpreter;
import io.fleak.zephflow.lib.sql.TestSQLUtils;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringConcatTests {

  @Test
  public void testStringConcatWithMinusSign() {
    var sql =
        "SELECT \n"
            + "    concat(doi, '-', \"chunk-id\") as id1,\n"
            + "    doi || '-' || \"chunk-id\" as id2,\n"
            + "    chunk,\n"
            + "    title,\n"
            + "    source,\n"
            + "    primary_category,\n"
            + "    published,\n"
            + "    updated\n"
            + "FROM events";
    System.out.println("SQL: \n" + sql);
    var rows = runSQL(sql).toList();
    Assertions.assertEquals(1, rows.size());

    for (var row : rows) {
      var rowMap = row.asMap();
      System.out.println(rowMap.get("id1").toString());
      Assertions.assertEquals(rowMap.get("id1"), rowMap.get("id2"));
    }
  }

  @Test
  public void testLookupAndStringConcat() {
    var rows = runSQL("select id::text || 'test' from EventS").toList();
    Assertions.assertEquals(1, rows.size());

    for (var row : rows) {
      Assertions.assertTrue(row.asMap().get("col_1").toString().contains("test"));
    }
  }

  private static Stream<Row> runSQL(String sql) {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    return TestSQLUtils.runSQL(
        Catalog.fromMap(
            Map.of("events", Table.ofListOfMaps(typeSystem, "events", List.of(getJSONEvent())))),
        sql);
  }

  private static Map getJSONEvent() {
    var eventString =
        "{\n"
            + "    \"doi\": 1910.01108,\n"
            + "    \"chunk-id\": 0,\n"
            + "    \"chunk\": \"DistilBERT, a distilled version of BERT: smaller,\\\\nfaster, cheaper and lighter\\\\nVictor SANH, Lysandre DEBUT, Julien CHAUMOND, Thomas WOLF\\\\nHugging Face\\\\n{victor,lysandre,julien,thomas}@huggingface.co\\\\nAbstract\\\\nAs Transfer Learning from large-scale pre-trained models becomes more prevalent\\\\nin Natural Language Processing (NLP), operating these large models in on-theedge and/or under constrained computational training or inference budgets remains\\\\nchallenging. In this work, we propose a method to pre-train a smaller generalpurpose language representation model, called DistilBERT, which can then be \\\\ufb01netuned with good performances on a wide range of tasks like its larger counterparts.\\\\nWhile most prior work investigated the use of distillation for building task-speci\\\\ufb01c\\\\nmodels, we leverage knowledge distillation during the pre-training phase and show\\\\nthat it is possible to reduce the size of a BERT model by 40%, while retaining 97%\\\\nof its language understanding capabilities and being 60% faster. To leverage the\\\\ninductive biases learned by larger models during pre-training, we introduce a triple\\\\nloss combining language modeling, distillation and cosine-distance losses. Our\\\\nsmaller, faster and lighter model is cheaper to pre-train and we demonstrate its\",\n"
            + "    \"id\": 1910.01108,\n"
            + "    \"title\": \"DistilBERT, a distilled version of BERT: smaller, faster, cheaper and lighter\",\n"
            + "    \"summary\": \"As Transfer Learning from large-scale pre-trained models becomes more\\\\nprevalent in Natural Language Processing (NLP), operating these large models in\\\\non-the-edge and/or under constrained computational training or inference\\\\nbudgets remains challenging. In this work, we propose a method to pre-train a\\\\nsmaller general-purpose language representation model, called DistilBERT, which\\\\ncan then be fine-tuned with good performances on a wide range of tasks like its\\\\nlarger counterparts. While most prior work investigated the use of distillation\\\\nfor building task-specific models, we leverage knowledge distillation during\\\\nthe pre-training phase and show that it is possible to reduce the size of a\\\\nBERT model by 40%, while retaining 97% of its language understanding\\\\ncapabilities and being 60% faster. To leverage the inductive biases learned by\\\\nlarger models during pre-training, we introduce a triple loss combining\\\\nlanguage modeling, distillation and cosine-distance losses. Our smaller, faster\\\\nand lighter model is cheaper to pre-train and we demonstrate its capabilities\\\\nfor on-device computations in a proof-of-concept experiment and a comparative\\\\non-device study.\",\n"
            + "    \"source\": \"http://arxiv.org/pdf/1910.01108\",\n"
            + "    \"authors\": [\n"
            + "      \"Victor Sanh\",\n"
            + "      \"Lysandre Debut\",\n"
            + "      \"Julien Chaumond\",\n"
            + "      \"Thomas Wolf\"\n"
            + "    ],\n"
            + "    \"categories\": [\n"
            + "      \"cs.CL\"\n"
            + "    ],\n"
            + "    \"comment\": \"February 2020 - Revision: fix bug in evaluation metrics, updated\\\\n  metrics, argumentation unchanged. 5 pages, 1 figure, 4 tables. Accepted at\\\\n  the 5th Workshop on Energy Efficient Machine Learning and Cognitive Computing\\\\n  - NeurIPS 2019\",\n"
            + "    \"journal_ref\": null,\n"
            + "    \"primary_category\": \"cs.CL\",\n"
            + "    \"published\": 20191002,\n"
            + "    \"updated\": 20200301,\n"
            + "    \"references\": [\n"
            + "      {\n"
            + "        \"id\": \"1910.01108\"\n"
            + "      }\n"
            + "    ]\n"
            + "  }";

    return JsonUtils.fromJsonString(eventString, new TypeReference<>() {});
  }
}
