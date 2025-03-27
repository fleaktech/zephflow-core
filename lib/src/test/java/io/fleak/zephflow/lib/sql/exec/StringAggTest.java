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

public class StringAggTest {

    @Test
    public void testStringAggNullEntry() {
        /*
         * The input document has 2 rows at maches->metadata, but only one has a text field.
         * This means that one row has a string and the other has a null.
         * We need to test that this null doesn't cause errors in the string_agg.
         *
         * The bug here was that the AggregateScan used toUnmodifiableList which doesn't allow list.
         */
        var sql = "SELECT string_agg(matches -> 'metadata'->'text'::text, ',') AS context\n" +
                "        FROM events, json_array_elements(query_output -> 'matchList') AS matches";

        var sqlInterpreter = SQLInterpreter.defaultInterpreter();
        var typeSystem = sqlInterpreter.getTypeSystem();

        var events = JsonUtils.fromJsonString(INPUT, new TypeReference<Map>() {});
        var rows = TestSQLUtils.runSQL(Catalog.fromMap(
                Map.of("events", Table.ofListOfMaps(typeSystem, "events", List.of(events)))
        ),
                sql);

        Assertions.assertEquals(1, rows.toList().size());
    }
    private static Stream<Row> runSQL(String sql) {
        var sqlInterpreter = SQLInterpreter.defaultInterpreter();
        var typeSystem = sqlInterpreter.getTypeSystem();

        return TestSQLUtils.runSQL(
                Catalog.fromMap(
                        Map.of(
                                "events",
                                Table.ofListOfMaps(
                                        typeSystem,
                                        "events",
                                        List.of(
                                                Map.of("log_entry", "127.0.0.1 - GET /index.html HTTP/1.1"),
                                                Map.of("log_entry", "192.168.1.1 - - [26/Mar/2023:10:00:00 +0000]"),
                                                Map.of("log_entry", "Contact us at support@example.com for assistance"),
                                                Map.of("log_entry", "2024-08-29 14:25:30,000 - INFO - User logged in")
                                        ))
                        )),
                sql);
    }
    public static String INPUT = "{\n" +
            "  \"chunk\": \"which team won 2022 world cup\",\n" +
            "  \"query_output\": {\n" +
            "    \"namespace\": \"\",\n" +
            "    \"matchList\": [\n" +
            "      {\n" +
            "        \"score\": 0.8455295562744141,\n" +
            "        \"metadata\": {\n" +
            "          \"pc_write_ts\": 1707157549521,\n" +
            "          \"text\": \"The 2022 World Cup concluded with Argentina emerging as the champions. In the final match, Argentina initially held a 2-0 lead over France, but France's Kylian Mbappé turned the tide with two goals in the second half. With just minutes remaining, Argentina's star player Lionel Messi broke the tie with another goal. However, Mbappé made history by completing a hat trick with a late penalty, leading to a 3-3 score.\\n\\nThe match was ultimately decided in a tense penalty shootout, where Argentina triumphed. Emiliano Martínez, Argentina's goalkeeper, made a critical save, and French winger Kingsley Coman missed his penalty, resulting in a final score of 4-2 in penalties for Argentina.\\n\\nArgentina's journey in the tournament began with a surprising loss to Saudi Arabia, but they rebounded and won every subsequent game. Their final opponent, defending champion France, struggled to maintain momentum throughout the match, with Mbappé's hat trick nearly turning the game around. The pressure culminated in the penalty shootout, affecting the French team's performance.\\n\\nFans around the world celebrated Argentina's victory, with hundreds of thousands gathering in Buenos Aires to join the festivities. Additionally, Croatia secured third place by defeating Morocco 2-1 in another match. Morocco's historic run as the first African and Arab team to reach the World Cup semifinals was acknowledged.\\n\\nLooking ahead, the 2026 World Cup will be hosted by the United States, Canada, and Mexico. Soccer enthusiasts can continue to enjoy matches across Europe, featuring favorite players like Messi.\\n\\nIn summary, the 2022 World Cup concluded with Argentina as the champions after an intense final match and penalty shootout against France.\"\n" +
            "        },\n" +
            "        \"id\": \"e30863ad-77b7-4772-9f57-efd5491e17aa\",\n" +
            "        \"values\": []\n" +
            "      },\n" +
            "      {\n" +
            "        \"score\": 0.7431516647338867,\n" +
            "        \"metadata\": {\n" +
            "          \"movie\": \"Invictus\",\n" +
            "          \"description\": \"Invictus is a movie in genre Drama,History originally in language en. The overview of the movie is as decribed here: Newly elected President Nelson Mandela knows his nation remains racially and economically divided in the wake of apartheid. Believing he can bring his people together through the universal language of sport, Mandela rallies South Africa's rugby team as they make their historic run to the 1995 Rugby World Cup Championship match.. It was released on 2009-12-10 with popularity 17.82 and vote_average 7.2 backed by 3534 vote_count\"\n" +
            "        },\n" +
            "        \"id\": \"22954\",\n" +
            "        \"values\": []\n" +
            "      }\n" +
            "    ],\n" +
            "    \"usage\": {\n" +
            "      \"readUnit\": 6\n" +
            "    }\n" +
            "  },\n" +
            "  \"embedding_object\": {\n" +
            "    \"model\": \"text-embedding-ada-002\",\n" +
            "    \"data\": [\n" +
            "      {\n" +
            "        \"index\": 0,\n" +
            "        \"embedding\": [      \n" +
            "          -0.0042844294,\n" +
            "          0.003963524,\n" +
            "          0.00402156\n" +
            "        ],\n" +
            "        \"object\": \"embedding\"\n" +
            "      }\n" +
            "    ],\n" +
            "    \"usage\": {\n" +
            "      \"total_tokens\": 8,\n" +
            "      \"prompt_tokens\": 8\n" +
            "    },\n" +
            "    \"object\": \"list\"\n" +
            "  }\n" +
            "}";
}
