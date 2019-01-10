package common;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;

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
	private boolean recordProgress;
	private String pluginName;

	public MongoAdapter (Parameter p) {
		//TODO: make optional or merge
//		targetstore = DatabaseHandler.createDatastore("localhost", 27017, "cfashark");
		datastore = DatabaseHandler.createDatastore(Parameter.getInstance());
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

	public List<CodeEntityState> getCodeEntityStates(ObjectId commitId, ObjectId fileId, String type) {
		List<CodeEntityState> states = datastore.find(CodeEntityState.class)
				.field("commit_id").equal(commitId)
				.field("file_id").equal(fileId)
				.field("ce_type").equal(type)
				.order("start_line")
				.asList();
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
	
	public List<Commit> getCommits() {
		//TODO: add to cache?
		List<Commit> commits = datastore.find(Commit.class)
				.field("vcs_system_id").equal(vcs.getId())
				.project("code_entity_states", false)
				.order("author_date")
				.asList();
		
		return commits;
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
		datastore.find(VCSSystem.class)
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

}
