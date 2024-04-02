package how.hollow.producer;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemAnnouncementWatcher;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemBlobRetriever;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;
import how.hollow.producer.datamodel.Actor;
import how.hollow.producer.datamodel.Movie;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.Spy;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ProducerTest {
    static File publishDir = new File("/home/qbug/tmp", "publish-dir");
    @Spy
    HollowProducer.Publisher publisher;
    HollowProducer.Announcer announcer;
    HollowConsumer.BlobRetriever blobRetriever;

    private static final int total = 1_000_0;
    private static final int round = 10;
    private static final int numsInRound = total / round;

    @BeforeEach
    public void setup() {
        publishDir.mkdir();
        publisher = new HollowFilesystemPublisher(publishDir.toPath());
        announcer = new HollowFilesystemAnnouncer(publishDir.toPath());
        blobRetriever = new HollowFilesystemBlobRetriever(publishDir.toPath());
    }

    @Test
    public void should_produce_in_10_batches_when_every_round_changed() {
        HollowProducer.Incremental incremental = getIncremental(false);
        for (int i = 0; i < round; i++) {
            final int idx = i;
            long version = incremental.runIncrementalCycle(state -> {
                generateMovies(idx * numsInRound, numsInRound, "movie_").forEach(state::addOrModify);
            });
            System.out.println(version);
        }
//        verify(publisher, times(2)).publish(any(HollowProducer.PublishArtifact.class));
    }

    @Test
    public void should_not_produce_at_last_in_10_batches_when_one_round_not_changed() {
        // Publish dataset for testing case
        final HollowProducer.Incremental incremental = getIncremental(true);
        // Please run previous test case to get the latest version and set here
        final long restoreVersion = 20240403122440010L;

        long version = 0;
        for (int i = 0; i < round; i++) {
            final int idx = i;
            version = incremental.runIncrementalCycle(state -> {
                generateMovies(idx * numsInRound, numsInRound, "movie_").forEach(state::addOrModify);
            });
        }

        HollowProducer.Incremental newIncremental = getIncremental(true);
        // Restore to make some rounds in next cycle not changed
        newIncremental.restore(restoreVersion, blobRetriever);
        verify(publisher, times(0)).publish(any(HollowProducer.PublishArtifact.class));

        // This round not changed
        long newVersion = newIncremental.runIncrementalCycle(state -> {
            generateMovies(0, numsInRound, "changed_movie_").forEach(state::addOrModify);
        });
        verify(publisher, times(4)).publish(any(HollowProducer.PublishArtifact.class));
        // Remaining movies are not changed
        for (int i = 1; i < round; i++) {
            final int idx = i;
            long roundVersion = newIncremental.runIncrementalCycle(state -> {
                generateMovies(idx * numsInRound, numsInRound, "movie_").forEach(state::addIfAbsent);
            });
            System.out.println("a round");
        }

        // Version changed because movies' titles changed
        assertNotEquals(version, newVersion);
        // Do not publish any artifact since line 85
        verify(publisher, times(4)).publish(any(HollowProducer.PublishArtifact.class));
    }

    private HollowProducer.Incremental getIncremental(boolean mockPublisher) {
        if (mockPublisher) {
            publisher = Mockito.mock(HollowProducer.Publisher.class);
            doAnswer(invocationOnMock -> {
                HollowProducer.PublishArtifact argument = invocationOnMock.getArgument(0);
                System.out.println(argument.getPath());
                return null;
            }).when(publisher).publish(any(HollowProducer.PublishArtifact.class));
        }

        HollowProducer.Incremental incremental = HollowProducer.withPublisher(publisher)
                .withAnnouncer(announcer)
                // set it so that publisher will only publish snapshot at beginning and at last
                .withNumStatesBetweenSnapshots(round - 2)
                .buildIncremental();
        incremental.initializeDataModel(Movie.class);
        return incremental;
    }

    List<Movie> generateMovies(int startId, int cnt, String titlePrefix) {
        final Set<Actor> empty = new HashSet<>();
        return IntStream.range(startId, startId + cnt)
                .mapToObj(id -> new Movie(id, titlePrefix +id, empty))
                .collect(Collectors.toList());
    }
}
