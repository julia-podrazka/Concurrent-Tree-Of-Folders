#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>

#include "Tree.h"

struct Tree {
    HashMap *subfolders;
    
    pthread_mutex_t lock;
    pthread_cond_t readers;
    pthread_cond_t writers;
    // Condtion on which a node is going to wait until all operations working
    // or waiting in node are executed.
    pthread_cond_t wait_for_node;
    int rcount, wcount, rwait, wwait;
    // Helps with recognising if a reader/writer should go to critical section,
    // especially after being awaken.
    int change;
};

static void entry_protocole_reader(Tree *tree) {

    if (pthread_mutex_lock(&tree->lock) != 0)
        syserr("lock failed");

    while (tree->change <= 0 && (tree->wcount > 0 || tree->wwait > 0)) {
        tree->rwait++;
        if (pthread_cond_wait(&tree->readers, &tree->lock) != 0)
            syserr("cond wait failed");
        tree->rwait--;
    }

    tree->rcount++;

    if (tree->change > 0)
        tree->change--;

    if (tree->change > 0) {
        if (pthread_cond_signal(&tree->readers) != 0)
            syserr("cond signal failed");
    }

    if (pthread_mutex_unlock(&tree->lock) != 0)
        syserr("unlock failed");

}

static void exit_protocole_reader(Tree *tree) {

    if (pthread_mutex_lock(&tree->lock) != 0)
        syserr("lock failed");

    tree->rcount--;

    if (tree->rcount == 0 && tree->wwait > 0) {
        tree->change = -1;
        if (pthread_cond_signal(&tree->writers) != 0)
            syserr("cond signal failed");
    } else if (tree->rcount == 0) {
        if (pthread_cond_signal(&tree->wait_for_node) != 0)
            syserr("cond signal failed");
    }

    if (pthread_mutex_unlock(&tree->lock) != 0)
        syserr("unlock failed");

}

static void entry_protocole_writer(Tree *tree) {

    if (pthread_mutex_lock(&tree->lock) != 0)
        syserr("lock failed");

    while (tree->change != -1 && (tree->wcount > 0 || tree->rcount > 0)) {
        tree->wwait++;
        if (pthread_cond_wait(&tree->writers, &tree->lock) != 0)
            syserr("cond wait failed");
        tree->wwait--;
    }

    tree->wcount++;
    tree->change = 0;

    if (pthread_mutex_unlock(&tree->lock) != 0)
        syserr("unlock failed");

}

static void exit_protocole_writer(Tree *tree) {

    if (pthread_mutex_lock(&tree->lock) != 0)
        syserr("lock failed");

    tree->wcount--;

    if (tree->rwait > 0) {
        tree->change = tree->rwait;
        if (pthread_cond_signal(&tree->readers) != 0)
            syserr("cond signal failed");
    } else if (tree->wwait > 0) {
        tree->change = -1;
        if (pthread_cond_signal(&tree->writers) != 0)
            syserr("cond signal failed");
    } else if (pthread_cond_signal(&tree->wait_for_node) != 0) {
            syserr("cond signal failed");
    }

    if (pthread_mutex_unlock(&tree->lock) != 0)
        syserr("unlock failed");

}

// Waits until all operations in node are done.
static void wait_for_operations_in_node(Tree *tree) {

    if (pthread_mutex_lock(&tree->lock) != 0)
        syserr ("lock failed");

    while (tree->rcount > 0 || tree->rwait > 0 || tree->wcount > 0 ||
           tree->wwait > 0) {
        if (pthread_cond_wait(&tree->wait_for_node, &tree->lock) != 0)
            syserr ("cond wait failed");
    }

    if (pthread_mutex_unlock(&tree->lock) != 0)
        syserr ("unlock failed");

}

static void init(Tree *tree) {

    if (pthread_mutex_init(&tree->lock, 0) != 0)
        syserr("mutex init failed");
    if (pthread_cond_init(&tree->readers, 0) != 0)
        syserr("cond init 1 failed");
    if (pthread_cond_init(&tree->writers, 0) != 0)
        syserr("cond init 2 failed");
    if (pthread_cond_init(&tree->wait_for_node, 0) != 0)
        syserr("cond init 3 failed");

    tree->rcount = 0;
    tree->wcount = 0;
    tree->rwait = 0;
    tree->wwait = 0;
    tree->change = 0;

}

Tree* tree_new() {

    Tree *new_tree = malloc(sizeof(Tree));
    if (new_tree == NULL) fatal("malloc failed");

    new_tree->subfolders = hmap_new();
    init(new_tree);

    return new_tree;

}

static void destroy(Tree *tree) {

    if (pthread_cond_destroy(&tree->readers) != 0)
        syserr("cond destroy 1 failed");
    if (pthread_cond_destroy(&tree->writers) != 0)
        syserr("cond destroy 2 failed");
    if (pthread_cond_destroy(&tree->wait_for_node) != 0)
        syserr("cond destroy 3 failed");
    if (pthread_mutex_destroy(&tree->lock) != 0)
        syserr("mutex destroy failed");

}

void tree_free(Tree *tree) {

    const char* key;
    void* value;
    HashMapIterator it = hmap_iterator(tree->subfolders);
    while (hmap_next(tree->subfolders, &it, &key, &value))
        tree_free(value);

    hmap_free(tree->subfolders);
    destroy(tree);
    free(tree);

}

// Iterates to a folder having the same directory as subpath; in the end
// *next_component points to a tree representing this folder. In each node
// we are considered as a reader. In the end, the last node is still a reader.
// Returns ENOENT if no such subpath exists and 0 if it does.
static int iterate_to_folder(const char *path_to_parent, Tree **next_component) {

    Tree *wait_next_component;
    char component[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    const char *subpath = path_to_parent;
    entry_protocole_reader(*next_component);
    while ((subpath = split_path(subpath, component))) {
        wait_next_component = hmap_get((*next_component)->subfolders, component);
        if (wait_next_component == NULL) {
            exit_protocole_reader(*next_component);
            return ENOENT;
        }
        entry_protocole_reader(wait_next_component);
        exit_protocole_reader(*next_component);
        *next_component = wait_next_component;
    }
    return 0;

}

char* tree_list(Tree *tree, const char* path) {

    if (!is_path_valid(path)) return NULL;

    Tree *next_component = tree;
    if (iterate_to_folder(path, &next_component) == ENOENT) return NULL;

    char *result = make_map_contents_string(next_component->subfolders);
    exit_protocole_reader(next_component);
    return result;

}

int tree_create(Tree *tree, const char* path) {

    if (!is_path_valid(path)) return EINVAL;
    if (strcmp(path, "/") == 0) return EEXIST;

    char new_subfolder[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char new_subfolder_parent[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char *path_to_parent = make_path_to_parent(path, new_subfolder);
    char *path_to_grandparent =
            make_path_to_parent(path_to_parent, new_subfolder_parent);
    free(path_to_parent);

    Tree *parent = tree;
    // If path_to_granparent is NULL then path_to_parent is "/".
    if (path_to_grandparent == NULL) {
        entry_protocole_writer(parent);
    } else {
        // We iterate to grandparent of a node to create because as we enter
        // a parent node we are considered a writer.
        Tree *next_component = tree;
        int code = iterate_to_folder(path_to_grandparent, &next_component);
        free(path_to_grandparent);
        if (code == ENOENT) return ENOENT;

        parent = hmap_get(next_component->subfolders, new_subfolder_parent);
        if (parent == NULL) {
            exit_protocole_reader(next_component);
            return ENOENT;
        }
        entry_protocole_writer(parent);
        exit_protocole_reader(next_component);
    }

    if (hmap_get(parent->subfolders, new_subfolder) != NULL) {
        exit_protocole_writer(parent);
        return EEXIST;
    }

    Tree *new_node = malloc(sizeof(Tree));
    if (new_node == NULL) {
        exit_protocole_writer(parent);
        fatal("malloc failed");
    }

    new_node->subfolders = hmap_new();
    init(new_node);
    hmap_insert(parent->subfolders, new_subfolder, new_node);

    exit_protocole_writer(parent);

    return 0;

}

// Removes and frees node without freeing its subfolders.
static void remove_node(Tree *node, Tree *next_component, char folder[]) {

    destroy(node);
    free(node);
    hmap_remove(next_component->subfolders, folder);

}

int tree_remove(Tree *tree, const char *path) {

    if (strcmp(path, "/") == 0) return EBUSY;
    if (!is_path_valid(path)) return EINVAL;

    char folder_to_remove[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char folder_to_remove_parent[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char *path_to_parent = make_path_to_parent(path, folder_to_remove);
    char *path_to_grandparent =
            make_path_to_parent(path_to_parent, folder_to_remove_parent);
    free(path_to_parent);

    Tree *parent = tree;

    // If path_to_granparent is NULL then path_to_parent is "/".
    if (path_to_grandparent == NULL) {
        entry_protocole_writer(parent);
    } else {
        // We iterate to grandparent of a node to create because as we enter
        // a parent node we are considered a writer.
        Tree *next_component = tree;
        int code = iterate_to_folder(path_to_grandparent, &next_component);
        free(path_to_grandparent);
        if (code == ENOENT) return ENOENT;

        parent = hmap_get(next_component->subfolders, folder_to_remove_parent);
        if (parent == NULL) {
            exit_protocole_reader(next_component);
            return ENOENT;
        }
        entry_protocole_writer(parent);
        exit_protocole_reader(next_component);
    }

    Tree *node_to_remove =
            hmap_get(parent->subfolders, folder_to_remove);

    if (node_to_remove == NULL) {
        exit_protocole_writer(parent);
        return ENOENT;
    }

    wait_for_operations_in_node(node_to_remove);

    if (hmap_size(node_to_remove->subfolders) != 0) {
        exit_protocole_writer(parent);
        return ENOTEMPTY;
    }

    hmap_free(node_to_remove->subfolders);
    remove_node(node_to_remove, parent, folder_to_remove);

    exit_protocole_writer(parent);

    return 0;

}

// Returns a new string with a path to lowest common ancestor in source and target.
static char *find_lowest_common_ancestor(const char *source, const char *target) {

    unsigned long min_len;
    unsigned long source_len = strlen(source);
    unsigned long target_len = strlen(target);
    if (source_len < target_len)
        min_len = source_len;
    else
        min_len = target_len;

    char *common_ancestor_path = malloc(min_len);

    unsigned long i = 0;
    while (i < min_len && source[i] == target[i]) {
        common_ancestor_path[i] = source[i];
        i++;
    }

    while(source[i - 1] != '/')
        i--;

    common_ancestor_path = realloc(common_ancestor_path, (i + 1));
    common_ancestor_path[i] = '\0';

    return common_ancestor_path;

}

// Iterates analogically to a function iterate_to_folder additionally
// changing path_source and path_target as we iterate.
static int iterate_with_paths(const char *lowest_ancestor, char **path_source,
                              char **path_target, Tree **next_component) {

    Tree *wait_next_component;
    char component[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    entry_protocole_reader(*next_component);
    const char *subpath = lowest_ancestor;
    while ((subpath = split_path(subpath, component))) {
        *path_source = *path_source + strlen(component) + 1;
        *path_target = *path_target + strlen(component) + 1;
        wait_next_component = hmap_get((*next_component)->subfolders, component);
        if (wait_next_component == NULL) {
            exit_protocole_reader(*next_component);
            return ENOENT;
        }
        entry_protocole_reader(wait_next_component);
        exit_protocole_reader(*next_component);
        *next_component = wait_next_component;
    }
    return 0;

}

// Waits until all nodes in a tree have no operations done or waiting.
static void wait_for_all_nodes_in_subtree(Tree *tree) {

    wait_for_operations_in_node(tree);
    const char *key;
    void *value;
    HashMapIterator it = hmap_iterator(tree->subfolders);
    while (hmap_next(tree->subfolders, &it, &key, &value))
        wait_for_all_nodes_in_subtree(value);

}

// Iterates analogically to a function iterate_to_folder but all nodes
// are considered as writers.
static int iterate_to_folder_writer(const char *path_to_parent, Tree **next_component) {

    Tree *wait_next_component;
    char component[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    const char *subpath = path_to_parent;
    entry_protocole_writer(*next_component);
    while ((subpath = split_path(subpath, component))) {
        wait_next_component = hmap_get((*next_component)->subfolders, component);
        if (wait_next_component == NULL) {
            exit_protocole_writer(*next_component);
            return ENOENT;
        }
        entry_protocole_writer(wait_next_component);
        exit_protocole_writer(*next_component);
        *next_component = wait_next_component;
    }
    return 0;

}

int tree_move(Tree *tree, const char *source, const char *target) {

    if (strcmp(source, "/") == 0) return EBUSY;
    if (strcmp(target, "/") == 0) return EEXIST;
    if (!is_path_valid(source) || !is_path_valid(target)) return EINVAL;
    // If target is a subfolder of a source, function tree_move returns -1.
    if (strlen(target) > strlen(source) &&
        strncmp(source, target, strlen(source)) == 0) return -1;
    // If source and target are the same folders and they exist,
    // no move is needed.
    if (strcmp(source, target) == 0) return 0;

    // We will want to block lowest common ancestor first, so we don't
    // have a deadlock with two tree_move.
    char *lowest_ancestor = find_lowest_common_ancestor(source, target);

    Tree *lowest_ancestor_tree = tree;
    char folder_lowest_ancestor[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char *path_to_parent_lowest_ancestor =
            make_path_to_parent(lowest_ancestor, folder_lowest_ancestor);

    char folder_to_move_to[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char *path_target = make_path_to_parent(target, folder_to_move_to);
    char folder_to_move[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char *path_source = make_path_to_parent(source, folder_to_move);

    if (path_to_parent_lowest_ancestor == NULL) {
        entry_protocole_writer(lowest_ancestor_tree);
    } else {
        Tree *next_component = tree;
        int code = iterate_with_paths(path_to_parent_lowest_ancestor, &path_source,
                         &path_target, &next_component);
        free(path_to_parent_lowest_ancestor);
        if (code == ENOENT) return ENOENT;
        path_source = path_source + strlen(folder_lowest_ancestor) + 1;
        path_target = path_target + strlen(folder_lowest_ancestor) + 1;
        lowest_ancestor_tree = hmap_get(next_component->subfolders,
                                        folder_lowest_ancestor);
        if (lowest_ancestor_tree == NULL) {
            exit_protocole_reader(next_component);
            return ENOENT;
        }

        entry_protocole_writer(lowest_ancestor_tree);
        exit_protocole_reader(next_component);
    }

    free(lowest_ancestor);

    bool target_parent_as_lowest_ancestor = false;
    bool source_parent_as_lowest_ancestor = false;

    Tree *parent_target = lowest_ancestor_tree;
    char component_target[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    const char *subpath_target = split_path(path_target, component_target);

    if (subpath_target == NULL) {
        target_parent_as_lowest_ancestor = true;
    } else {
        parent_target =
                hmap_get(lowest_ancestor_tree->subfolders, component_target);
        if (parent_target == NULL) {
            exit_protocole_writer(lowest_ancestor_tree);
            return ENOENT;
        }

        int code = iterate_to_folder_writer(subpath_target, &parent_target);
        if (code == ENOENT) {
            exit_protocole_writer(lowest_ancestor_tree);
            return code;
        }
    }

    if (hmap_get(parent_target->subfolders, folder_to_move_to) != NULL) {
        exit_protocole_writer(lowest_ancestor_tree);
        if (!target_parent_as_lowest_ancestor)
            exit_protocole_writer(parent_target);
        return EEXIST;
    }

    Tree *parent_source = lowest_ancestor_tree;
    char component_source[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    const char *subpath_source = split_path(path_source, component_source);

    if (subpath_source == NULL) {
        source_parent_as_lowest_ancestor = true;
    } else {
        parent_source =
                hmap_get(lowest_ancestor_tree->subfolders, component_source);
        if (parent_source == NULL) {
            exit_protocole_writer(lowest_ancestor_tree);
            if (!target_parent_as_lowest_ancestor)
                exit_protocole_writer(parent_target);
            return ENOENT;
        }

        int code = iterate_to_folder_writer(subpath_source, &parent_source);
        if (code == ENOENT) {
            exit_protocole_writer(lowest_ancestor_tree);
            if (!target_parent_as_lowest_ancestor)
                exit_protocole_writer(parent_target);
            return code;
        }
    }

    Tree *node_to_move = hmap_get(parent_source->subfolders, folder_to_move);
    if (node_to_move == NULL) {
        exit_protocole_writer(lowest_ancestor_tree);
        if (!target_parent_as_lowest_ancestor)
            exit_protocole_writer(parent_target);
        if (!source_parent_as_lowest_ancestor)
            exit_protocole_writer(parent_source);
        return ENOENT;
    }

    // If lowest_ancestor_tree is different from parent_source and parent_target
    // then we can unlock access to it, because we are already locked in
    // parent_source and parent_target.
    if (!target_parent_as_lowest_ancestor && !source_parent_as_lowest_ancestor)
        exit_protocole_writer(lowest_ancestor_tree);

    wait_for_all_nodes_in_subtree(node_to_move);

    Tree *new_node = malloc(sizeof(Tree));
    if (new_node == NULL) fatal("malloc failed");

    new_node->subfolders = node_to_move->subfolders;
    init(new_node);
    hmap_insert(parent_target->subfolders, folder_to_move_to, new_node);
    remove_node(node_to_move, parent_source, folder_to_move);

    // If parent_target and parent_source are the same node as
    // lowest_ancestor_tree, then exit protocol is called only once.
    if (!target_parent_as_lowest_ancestor || !source_parent_as_lowest_ancestor) {
        exit_protocole_writer(parent_source);
        exit_protocole_writer(parent_target);
    } else {
        exit_protocole_writer(lowest_ancestor_tree);
    }

    return 0;

}
