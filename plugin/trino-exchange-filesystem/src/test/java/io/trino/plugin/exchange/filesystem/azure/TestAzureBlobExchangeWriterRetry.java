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
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
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
import static org.assertj.core.api.Assertions.assertThat;

public class TestAzureBlobExchangeWriterRetry
{
    private static final int BLOCK_SIZE = 4;

    @Test
    public void testDirectUploadRetryAfterLostResponse()
    {
        LostFirstResponseServer server = new LostFirstResponseServer();
        ExchangeStorageWriter writer = new AzureExchangeStorageWriter(server.blockBlobClient(), BLOCK_SIZE);
        getFutureValue(writer.write(Slices.utf8Slice("abc")));
        getFutureValue(writer.finish());
        assertThat(server.committedBlobs).hasSize(1);
    }

    @Test
    public void testCommitBlockListRetryAfterLostResponse()
    {
        LostFirstResponseServer server = new LostFirstResponseServer();
        ExchangeStorageWriter writer = new AzureExchangeStorageWriter(server.blockBlobClient(), BLOCK_SIZE);
        getFutureValue(writer.write(Slices.utf8Slice("abcdefgh")));
        getFutureValue(writer.write(Slices.utf8Slice("ijklmnop")));
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

        BlockBlobAsyncClient blockBlobClient()
        {
            HttpClient httpClient = this::send;
            return new BlobServiceClientBuilder()
                    .endpoint("https://account.blob.core.windows.net")
                    .httpClient(httpClient)
                    .retryOptions(new RequestRetryOptions(RetryPolicyType.FIXED, 2, (Integer) null, 1L, 1L, null))
                    .buildAsyncClient()
                    .getBlobContainerAsyncClient("container")
                    .getBlobAsyncClient("exchange/0_0.data")
                    .getBlockBlobAsyncClient();
        }

        private synchronized Mono<HttpResponse> send(HttpRequest request)
        {
            String query = request.getUrl().getQuery();
            boolean stageBlock = query != null && query.contains("blockid=");
            if (request.getHttpMethod() != HttpMethod.PUT || stageBlock) {
                return Mono.just(new StatusOnlyResponse(request, 201));
            }
            String blob = request.getUrl().getPath();
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
