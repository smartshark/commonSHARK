package common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.eclipse.jgit.api.BlameCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.blame.BlameResult;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;


public class GitAdapter {
	protected Repository repository;
	private HashMap<String, RevCommit> revisionCache = new HashMap<>();
	protected static Logger logger = (Logger) LoggerFactory.getLogger(MongoAdapter.class.getCanonicalName());

	public GitAdapter(String location) throws IOException {
		this.repository = FileRepositoryBuilder.create(
				new java.io.File(location));

	}
	
	public RevCommit getRevision(String hash) throws Exception {
		if (!revisionCache.containsKey(hash)) {
			try (RevWalk walk = new RevWalk(repository)) {
				RevCommit commit = walk.parseCommit(repository.resolve(hash));
				walk.close();
				revisionCache.put(hash, commit);
			} catch (Exception e) {
				logger.error("Revision "+hash+ " could not be found in the git repository");
				logger.error("  "+e.getMessage());
			}
		}
		return revisionCache.get(hash);
    }
	
	public BlameResult getBlameResult(String hash, String path, boolean followRenames) throws Exception {
		BlameCommand blamer = new BlameCommand(repository);
		blamer.setStartCommit(getRevision(hash));
		blamer.setFilePath(path);
		//TODO: check differences in outcomes, as the setting may be unreliable in some cases
		blamer.setFollowFileRenames(followRenames);
		BlameResult blame = blamer.call();
		return blame;
	}

	public List<String> getOrderedRevisionHashes() {
		List<String> revisionHashes = new ArrayList<String>();
		try (Git git = new Git(repository)) {
            Iterable<RevCommit> revs = git.log().all().call();
            for (RevCommit rev : revs) {
            	revisionHashes.add(rev.getName());
            }
		} catch (Exception e) {
			e.printStackTrace();
		}

		Collections.reverse(revisionHashes);
		return revisionHashes;
	}

    public List<String> getFileContentAtCommit(String hash, String path) {
    	List<String> contents = new ArrayList<>();

        // now try to find a specific file
        try (TreeWalk treeWalk = new TreeWalk(repository)) {
        	RevTree tree = getRevision(hash).getTree();
            treeWalk.addTree(tree);
            treeWalk.setRecursive(true);
            treeWalk.setFilter(PathFilter.create(path));
            if (!treeWalk.next()) {
                throw new IllegalStateException("Did not find expected file '"+path+"'");
            }

            org.eclipse.jgit.lib.ObjectId objectId = treeWalk.getObjectId(0);
            ObjectLoader loader = repository.open(objectId);

            // and then one can the loader to read the file
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            loader.copyTo(stream);
            try (Scanner scanner = new Scanner(new ByteArrayInputStream(stream.toByteArray()))) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    // use line here
                    contents.add(line);
                }
            } 
		} catch (Exception e) {
			e.printStackTrace();
		}
	    return contents;
    }

	
}
