/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.exchange.filesystem.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import io.airlift.slice.Slices;
import io.trino.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.trino.plugin.exchange.filesystem.azure.AzureBlobFileSystemExchangeStorage.AzureExchangeStorageWriter;
import io.trino.plugin.exchange.filesystem.azure.TestAzureBlobExchangeReadFailure.StatusOnlyResponse;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Set;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.exchange.filesystem.azure.AzureBlobFileSystemExchangeStorage.createEmptyBlob;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAzureBlobExchangeWriterRetry
{
    private static final int BLOCK_SIZE = 4;

    @Test
    public void testDirectUploadRetryAfterLostResponse()
    {
        assertWriteSurvivesLostResponse("abc");
    }

    @Test
    public void testCommitBlockListRetryAfterLostResponse()
    {
        assertWriteSurvivesLostResponse("abcdefgh", "ijklmnop");
    }

    @Test
    public void testCreateEmptyFileRetryAfterLostResponse()
    {
        LostFirstResponseServer server = new LostFirstResponseServer();
        getFutureValue(createEmptyBlob(server.blobClient()));
        assertThat(server.committedBlobs).hasSize(1);
    }

    private static void assertWriteSurvivesLostResponse(String... chunks)
    {
        LostFirstResponseServer server = new LostFirstResponseServer();
        ExchangeStorageWriter writer = new AzureExchangeStorageWriter(server.blobClient().getBlockBlobAsyncClient(), BLOCK_SIZE);
        for (String chunk : chunks) {
            getFutureValue(writer.write(Slices.utf8Slice(chunk)));
        }
        getFutureValue(writer.finish());
        assertThat(server.committedBlobs).hasSize(1);
    }

    /**
     * Applies the first write of each blob, then fails it with 503 as if the response was lost, so the client retries.
     * Rejects a write with {@code If-None-Match: *} when the blob exists, like Azure does.
     */
    private static class LostFirstResponseServer
    {
        private final Set<String> committedBlobs = new HashSet<>();

        BlobAsyncClient blobClient()
        {
            HttpClient httpClient = this::send;
            return new BlobServiceClientBuilder()
                    .endpoint("https://account.blob.core.windows.net")
                    .httpClient(httpClient)
                    .retryOptions(new RequestRetryOptions(RetryPolicyType.FIXED, 2, (Integer) null, 1L, 1L, null))
                    .buildAsyncClient()
                    .getBlobContainerAsyncClient("container")
                    .getBlobAsyncClient("exchange/0_0.data");
        }

        private synchronized Mono<HttpResponse> send(HttpRequest request)
        {
            String query = request.getUrl().getQuery();
            boolean stageBlock = query != null && query.contains("blockid=");
            String blob = request.getUrl().getPath();
            if (request.getHttpMethod() == HttpMethod.HEAD) {
                return Mono.just(new StatusOnlyResponse(request, committedBlobs.contains(blob) ? 200 : 404));
            }
            if (request.getHttpMethod() != HttpMethod.PUT || stageBlock) {
                return Mono.just(new StatusOnlyResponse(request, 201));
            }
            if (committedBlobs.contains(blob)) {
                if ("*".equals(request.getHeaders().getValue(HttpHeaderName.IF_NONE_MATCH))) {
                    return Mono.just(new StatusOnlyResponse(request, 409));
                }
                return Mono.just(new StatusOnlyResponse(request, 201));
            }
            committedBlobs.add(blob);
            return Mono.just(new StatusOnlyResponse(request, 503));
        }
    }
}
