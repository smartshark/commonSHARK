package common;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import de.ugoe.cs.smartshark.model.CFAState;
import de.ugoe.cs.smartshark.model.CodeEntityState;
import de.ugoe.cs.smartshark.model.Commit;
import de.ugoe.cs.smartshark.model.File;
import de.ugoe.cs.smartshark.model.FileAction;
import de.ugoe.cs.smartshark.model.Hunk;
import de.ugoe.cs.smartshark.model.HunkBlameLine;
import de.ugoe.cs.smartshark.model.PluginProgress;
import de.ugoe.cs.smartshark.model.VCSSystem;

public class MongoAdapter {
	private Datastore datastore;
	private Datastore targetstore;
	private VCSSystem vcs;

	private HashMap<String, File> fileCache = new HashMap<>();
	private HashMap<ObjectId, File> fileIdCache = new HashMap<>();
	private HashMap<String, Commit> commitCache = new HashMap<>();
	private HashMap<ObjectId, Commit> commitIdCache = new HashMap<>();
	private HashMap<ObjectId, List<HunkBlameLine>> hblCache = new HashMap<>();
	private HashMap<ObjectId, CFAState> cfaCache = new HashMap<>();
	private HashMap<ObjectId, CFAState> cfaEntityCache = new HashMap<>();
	private LinkedHashMap<ObjectId, ArrayList<FileAction>> fileActionsCache = new LinkedHashMap<>();
	private List<String> revisionHashes = new ArrayList<String>();
	private boolean recordProgress;
	private String pluginName;
	private Parameter parameter;
	protected static Logger logger = (Logger) LoggerFactory.getLogger(MongoAdapter.class.getCanonicalName());

	public MongoAdapter (Parameter p) {
		//TODO: make optional or merge
//		targetstore = DatabaseHandler.createDatastore("localhost", 27017, "cfashark");
		parameter = p;
		datastore = DatabaseHandler.createDatastore(parameter);
		targetstore = datastore;
	}
	
	public List<HunkBlameLine> getHunkBlameLines(Hunk h) {
		if (!hblCache.containsKey(h.getId())) {
			List<HunkBlameLine> blameLines = targetstore.find(HunkBlameLine.class)
    			.field("hunk_id").equal(h.getId()).asList();
			hblCache.put(h.getId(), blameLines);
		}
		return hblCache.get(h.getId());
	}

	public List<CodeEntityState> getCodeEntityStates(ObjectId commitId, ObjectId fileId) {
		List<CodeEntityState> states = new ArrayList<>();
		Commit commit = datastore.get(Commit.class, commitId);
		for (ObjectId id : commit.getCodeEntityStates()) {
			List<CodeEntityState> ces = datastore.find(CodeEntityState.class)
					.field("_id").equal(id)
					.field("file_id").equal(fileId)
//					.order("start_line")
					.asList();
			ces.sort(Comparator.comparing(CodeEntityState::getStartLine));
			states.addAll(ces);
		}
		return states;
	}
	
	public List<CodeEntityState> getCodeEntityStates(ObjectId commitId, ObjectId fileId, String type) {
		List<CodeEntityState> states = new ArrayList<>();
		Commit commit = datastore.get(Commit.class, commitId);
		for (ObjectId id : commit.getCodeEntityStates()) {
			List<CodeEntityState> ces = datastore.find(CodeEntityState.class)
					.field("_id").equal(id)
					.field("file_id").equal(fileId)
					.field("ce_type").equal(type)
//					.order("start_line")
					.asList();
			ces.sort(Comparator.comparing(CodeEntityState::getStartLine));
			states.addAll(ces);
		}
		return states;
	}

	public List<CodeEntityState> getCodeEntityStates(ObjectId commitId, String type) {
		List<CodeEntityState> states = new ArrayList<>();
		Commit commit = datastore.get(Commit.class, commitId);
		for (ObjectId id : commit.getCodeEntityStates()) {
			List<CodeEntityState> ces = datastore.find(CodeEntityState.class)
					.field("_id").equal(id)
					.field("ce_type").equal(type)
//					.order("start_line")
					.asList();
			ces.sort(Comparator.comparing(CodeEntityState::getStartLine));
			states.addAll(ces);
		}
		return states;
	}

	public List<CodeEntityState> getCodeEntityStates(ObjectId commitId) {
		List<CodeEntityState> states = new ArrayList<>();
		Commit commit = datastore.get(Commit.class, commitId);
		for (ObjectId id : commit.getCodeEntityStates()) {
			CodeEntityState ces = datastore.get(CodeEntityState.class, id);
			states.add(ces);
		}
		return states;
	}

	
	public File getFile(String path) {
		if (!fileCache.containsKey(path)) {
			File file = datastore.find(File.class)
				.field("vcs_system_id").equal(vcs.getId())
				.field("path").equal(path).get();
			fileCache.put(path, file);
			fileIdCache.put(file.getId(), file);
		}
		return fileCache.get(path);
	}

	public File getFile(ObjectId id) {
		if (!fileIdCache.containsKey(id)) {
			File file = datastore.get(File.class, id);
			fileCache.put(file.getPath(), file);
			fileIdCache.put(id, file);
		}
		return fileIdCache.get(id);
	}
	
	public void constructFileActionMap() {
		fileActionsCache.clear();
		logger.info("Constructing file action map..");
		logger.info("  Fetch commits..");
		List<Commit> commits = getCommits();
		int i = 0;
		int size = commits.size();

		for (Commit c : commits) {
			i++;
			//logger.info("  Processing: "+i+"/"+size+" "+c.getRevisionHash());
			//skip merges
			if (c.getParents()!=null && c.getParents().size()>1) {
				continue;
			}
			List<FileAction> actions = getActions(c);
			for (FileAction a : actions) {
				if (!fileActionsCache.containsKey(a.getFileId())) {
					fileActionsCache.put(a.getFileId(), new ArrayList<FileAction>());
				}
				fileActionsCache.get(a.getFileId()).add(a);
				if (a.getMode().equals("R") || a.getMode().equals("C")) {
					if (!fileActionsCache.containsKey(a.getOldFileId())) {
						fileActionsCache.put(a.getOldFileId(), new ArrayList<FileAction>());
					}
					fileActionsCache.get(a.getOldFileId()).add(a);
				}
			}
		}
		//TODO: link renamed files?
		logger.info("  Done!");
	}
	
	public List<FileAction> getActions(ObjectId fileId) {
		ArrayList<FileAction> actions = fileActionsCache.get(fileId);
		return actions;
	}

	public List<FileAction> getActionsFollowRenamesBackward(ObjectId fileId) {
		List<FileAction> actions = new ArrayList<>(fileActionsCache.get(fileId));
		FileAction first = actions.get(0);
		if (first.getMode().equals("R") || first.getMode().equals("C") ) {
			List<FileAction> followedActions = getActionsFollowRenamesBackward(first.getOldFileId());
			Date firstDate = getCommit(first.getCommitId()).getCommitterDate();
			followedActions = followedActions.stream()
					.filter(a -> getCommit(a.getCommitId()).getCommitterDate().before(firstDate))
					.filter(a->!(a.getMode().equals("C") && a.getOldFileId().equals(first.getFileId())))
					.collect(Collectors.toList());
			actions.addAll(0, followedActions);
		}
		
		actions = actions.stream()
				.filter(a->!((a.getMode().equals("C")
//						   || a.getMode().equals("R")
						   ) 
						   && a.getOldFileId().equals(fileId)))
				.collect(Collectors.toList());

		return actions;
	}

	public List<FileAction> getActionsFollowRenamesForward(ObjectId fileId) {
		ArrayList<FileAction> actions = new ArrayList<>(fileActionsCache.get(fileId));
		
		for (FileAction a : actions) {
        	File file = getFile(a.getFileId());
        	Commit commit = getCommit(a.getCommitId());

        	logger.info(""
        			+"  "+a.getMode()
        			+"  "+commit.getRevisionHash().substring(0,8)
        			+"  "+file.getPath());

		}

		
		FileAction last = actions.get(actions.size()-1);
		if (last.getMode().equals("R") && actions.size() > 1) {
			actions.remove(last);
			actions.addAll(getActionsFollowRenamesForward(last.getFileId()));
		}
		return actions;
	}

	
	public List<FileAction> getActionsFollowRenames(ObjectId fileId) {
		List<FileAction> actions = new ArrayList<>(fileActionsCache.get(fileId));
		
		FileAction first = actions.get(0);
		if (first.getMode().equals("R") || first.getMode().equals("C") ) {
			List<FileAction> followedActions = getActionsFollowRenamesBackward(first.getOldFileId());
			Date firstDate = getCommit(first.getCommitId()).getCommitterDate();
			followedActions = followedActions.stream()
					.filter(a -> getCommit(a.getCommitId()).getCommitterDate().before(firstDate))
					.filter(a->!(a.getMode().equals("C") && a.getOldFileId().equals(first.getFileId())))
					.collect(Collectors.toList());
			actions.addAll(0, followedActions);
		}

		for (FileAction a : actions) {
        	File file = getFile(a.getFileId());
        	Commit commit = getCommit(a.getCommitId());

        	logger.info(""
        			+"  "+a.getMode()
        			+"  "+commit.getRevisionHash().substring(0,8)
        			+"  "+file.getPath());

		}
		
		FileAction last = actions.get(actions.size()-1);
		if (last.getMode().equals("R")) {
			actions.remove(last);
			actions.addAll(getActionsFollowRenamesForward(last.getFileId()));
		}

		actions = actions.stream()
				.filter(a->!((a.getMode().equals("C")
//						   || a.getMode().equals("R")
						   )
						   && a.getOldFileId().equals(fileId)))
				.collect(Collectors.toList());

		return actions;
	}
	
	public List<FileAction> getActions(Commit commit) {
		List<FileAction> actions = datastore.find(FileAction.class)
    		.field("commit_id").equal(commit.getId()).asList();
		return actions;
	}
	
	public FileAction getAction(ObjectId commitId, ObjectId fileId) {
		FileAction cAction = datastore.find(FileAction.class)
			.field("commit_id").equal(commitId)
			.field("file_id").equal(fileId).get();
		return cAction;
	}

	public List<Hunk> getHunks(FileAction a) {
		List<Hunk> hunks = datastore.find(Hunk.class)
			.field("file_action_id").equal(a.getId()).asList();
		return hunks;
	}

	public void interpolateHunks(List<Hunk> hunks) {
        //-> double check off-by-one (0-based or 1-based)
        //  -> [old|new]StartLine is 0-based when [old|new]Lines = 0
        //     - hunk removed or added
        //     - only affects the corresponding side [old|new]
        //  -> [old|new]StartLine is 1-based when [old|new]Lines > 0
        //     - hunk modified
        //-> make sure it is consistent for old and new
        //  -> interpolate as necessary

		for (Hunk h : hunks) {
			if (h.getOldLines()==0) {
				h.setOldStart(h.getOldStart()+1);
			}
			if (h.getNewLines()==0) {
				h.setNewStart(h.getNewStart()+1);
			}
		}
	}
	
	public List<Commit> getCommitsNoCache() {
		List<Commit> commits = datastore.find(Commit.class)
				.field("vcs_system_id").equal(vcs.getId())
				.project("code_entity_states", false)
//				.order("committer_date")
				.asList();
		sortCommits(commits);
		return commits;
	}

	public List<Commit> getCommits() {
		//always hits the db but caches the commits for later use
		List<Commit> commits = datastore.find(Commit.class)
				.field("vcs_system_id").equal(vcs.getId())
				.project("code_entity_states", false)
//				.order("committer_date")
				.asList();
		sortCommits(commits);
		for (Commit commit : commits) {
			if (!commitIdCache.containsKey(commit.getId())) {
				commitCache.put(commit.getRevisionHash(), commit);
				commitIdCache.put(commit.getId(), commit);
			}
		}
		return commits;
	}

	private void sortCommits(List<Commit> commits) {
		if (revisionHashes.isEmpty()) {
			commits.sort(Comparator.comparing(Commit::getCommitterDate)
					.thenComparing(Commit::getAuthorDate));
		} else {
			commits.sort((e1, e2)
					-> revisionHashes.indexOf(e1.getRevisionHash())
		             - revisionHashes.indexOf(e2.getRevisionHash()));
		}
	}
	
	public Commit getCommit(String hash) {
		if (!commitCache.containsKey(hash)) {
			Commit commit = datastore.find(Commit.class)
				.field("vcs_system_id").equal(vcs.getId())
				.field("revision_hash").equal(hash)
				.project("code_entity_states", false)
				.get();
			commitCache.put(hash, commit);
			commitIdCache.put(commit.getId(), commit);
		}
		return commitCache.get(hash);
	}
	
	public Commit getCommit(ObjectId id) {
		if (!commitIdCache.containsKey(id)) {
//			Commit commit = datastore.get(Commit.class, id);
			Commit commit = datastore.find(Commit.class)
//					.field("vcs_system_id").equal(vcs.getId())
					.field("_id").equal(id)
					.project("code_entity_states", false)
					.get();
			commitCache.put(commit.getRevisionHash(), commit);
			commitIdCache.put(id, commit);
		}
		return commitIdCache.get(id);
	}

	public CFAState getCFAState(ObjectId id) {
		if (!cfaCache.containsKey(id)) {
			CFAState state = targetstore.get(CFAState.class, id);
			if (state == null) {
				return state;
			}
			cfaCache.put(id, state);
			cfaEntityCache.put(state.getEntityId(), state);
		}
		return cfaCache.get(id);
	}

	public CFAState getCFAStateForEntity(ObjectId id) {
		if (!cfaEntityCache.containsKey(id)) {
			CFAState state = targetstore.find(CFAState.class)
				.field("entity_id").equal(id)
				.get();
			if (state == null) {
				return state;
			}
			cfaEntityCache.put(id, state);
			cfaCache.put(state.getId(), state);
		}
		return cfaEntityCache.get(id);
	}

	public void saveCFAState(CFAState s) {
        targetstore.save(s);
        cfaCache.put(s.getId(), s);
        cfaEntityCache.put(s.getEntityId(), s);
	}

	public void flushCFACache() {
    	for (CFAState s : cfaCache.values()) {
        	targetstore.save(s);
    	}
    	//clear or keep?
    	cfaCache.clear();
    	cfaEntityCache.clear();
	}

	public void resetProgess(ObjectId targetId) {
		if (isRecordProgress()) {
			//TODO: instead of/in addition to deleting, set to "STARTED"?
			//      - keep the object between start and end?
			targetstore.delete(targetstore.find(PluginProgress.class)
					.field("plugin").equal(getPluginName())
					.field("project_id").equal(vcs.getProjectId())
					.field("target_id").equal(targetId));
		}
	}
	
	public void logProgess(ObjectId targetId, String type) {
		if (isRecordProgress()) {
			PluginProgress p = new PluginProgress();
			p.setPlugin(getPluginName());
			p.setTime(new Date());
			p.setProjectId(vcs.getProjectId());
			p.setTargetId(targetId);
			p.setStatus("DONE");
			p.setType(type);
			targetstore.save(p);
		}
	}
	
	public VCSSystem getVcs() {
		return vcs;
	}

	public void setVcs(VCSSystem vcs) {
		this.vcs = vcs;
	}
	
	public void setVcs(String url) {
		this.vcs = datastore.find(VCSSystem.class)
			.field("url").equal(url).get();
	}

	public Datastore getDatastore() {
		return datastore;
	}

	public void setDatastore(Datastore datastore) {
		this.datastore = datastore;
	}

	public Datastore getTargetstore() {
		return targetstore;
	}

	public void setTargetstore(Datastore targetstore) {
		this.targetstore = targetstore;
	}

	public Datastore getTargetstore(String hostname, int port, String database) {
		return DatabaseHandler.createDatastore(hostname, port, database);
	}

	public Datastore getTargetstore(String database) {
		return DatabaseHandler.createDatastore(parameter, database);
	}

	public boolean isRecordProgress() {
		return recordProgress;
	}

	public void setRecordProgress(boolean recordProgress) {
		this.recordProgress = recordProgress;
	}

	public String getPluginName() {
		return pluginName;
	}

	public void setPluginName(String pluginName) {
		this.pluginName = pluginName;
	}

	public List<String> getRevisionHashes() {
		return revisionHashes;
	}

	public void setRevisionHashes(List<String> revisionHashes) {
		this.revisionHashes = revisionHashes;
	}

}
