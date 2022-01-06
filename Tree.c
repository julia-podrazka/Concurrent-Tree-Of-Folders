#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "Tree.h"

struct Tree {
    char *folder;
    HashMap *subfolders;
};

Tree* tree_new() {

    Tree *new_tree = malloc(sizeof(Tree));
    if (new_tree == NULL)
        exit(1);
    new_tree->folder = strdup("/");
    new_tree->subfolders = hmap_new();

    return new_tree;

}

void tree_free(Tree* tree) {

    const char* key;
    void* value;
    HashMapIterator it = hmap_iterator(tree->subfolders);
    while (hmap_next(tree->subfolders, &it, &key, &value))
        tree_free(value);

    hmap_free(tree->subfolders);
    free(tree->folder);
    free(tree);

}

char* tree_list(Tree* tree, const char* path) {

    if (!is_path_valid(path))
        return NULL;

    Tree *next_component = tree;
    char component[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    const char *subpath = path;
    while ((subpath = split_path(subpath, component))) {
        next_component = hmap_get(next_component->subfolders, component);
        if (next_component == NULL)
            return NULL;
    }

    return make_map_contents_string(next_component->subfolders);

}

static int iterate_to_folder(const char *subpath, Tree **next_component) {

    char component[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    while ((subpath = split_path(subpath, component))) {
        *next_component = hmap_get((*next_component)->subfolders, component);
        if (*next_component == NULL)
            return ENOENT;
    }

    return 0;

}

int tree_create(Tree* tree, const char* path) {

    if (!is_path_valid(path))
        return EINVAL;

    if (strcmp(path, "/") == 0)
        return EEXIST;

    Tree *next_component = tree;
    //char component[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char new_subfolder[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char *path_to_parent = make_path_to_parent(path, new_subfolder);
    const char *subpath = path_to_parent;
    /*while ((subpath = split_path(subpath, component))) {
        next_component = hmap_get(next_component->subfolders, component);
        if (next_component == NULL) {
            free(path_to_parent);
            return ENOENT;
        }
    }*/

    int code = iterate_to_folder(subpath, &next_component);
    free(path_to_parent);
    if (code == ENOENT)
        return code;

    if (hmap_get(next_component->subfolders, new_subfolder) != NULL)
        return EEXIST;

    Tree *new_node = malloc(sizeof(Tree));
    if (new_node == NULL)
        exit(1);
    new_node->folder = strdup(path); // czy potrzebujemy całej ścieżki???
    new_node->subfolders = hmap_new();
    hmap_insert(next_component->subfolders, new_subfolder, new_node);

    return 0;

}

int tree_remove(Tree* tree, const char* path) {

    if (strcmp(path, "/") == 0)
        return EBUSY;
    if (!is_path_valid(path))
        return EINVAL;

    Tree *next_component = tree;
    char component[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char folder_to_remove[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char *path_to_parent = make_path_to_parent(path, folder_to_remove);
    const char *subpath = path_to_parent;
    while ((subpath = split_path(subpath, component))) {
        next_component = hmap_get(next_component->subfolders, component);
        if (next_component == NULL) {
            free(path_to_parent);
            return ENOENT;
        }
    }

    free(path_to_parent);

    Tree *node_to_remove =
            hmap_get(next_component->subfolders, folder_to_remove);
    if (node_to_remove == NULL)
        return ENOENT;
    if (hmap_size(node_to_remove->subfolders) != 0)
        return ENOTEMPTY;

    hmap_free(node_to_remove->subfolders);
    free(node_to_remove->folder);
    free(node_to_remove);
    hmap_remove(next_component->subfolders, folder_to_remove);

    return 0;

}

int tree_move(Tree* tree, const char* source, const char* target) {

    if (strcmp(source, "/") == 0)
        return EBUSY;
    // Czy zostawić to tutaj czy po sprawdzeniu, czy source istnieje?
    if (strcmp(target, "/") == 0)
        return EEXIST;

    if (!is_path_valid(source) || !is_path_valid(target))
        return EINVAL;

    if (strlen(target) > strlen(source) &&
        strncmp(source, target, strlen(source)) == 0)
        return -1;

    Tree *next_component_source = tree;
    char component_source[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char folder_to_move[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char *path_to_parent_source = make_path_to_parent(source, folder_to_move);
    const char *subpath_source = path_to_parent_source;
    while ((subpath_source = split_path(subpath_source, component_source))) {
        next_component_source =
                hmap_get(next_component_source->subfolders, component_source);
        if (next_component_source == NULL) {
            free(path_to_parent_source);
            return ENOENT;
        }
    }

    free(path_to_parent_source);

    Tree *node_to_move =
            hmap_get(next_component_source->subfolders, folder_to_move);
    if (node_to_move == NULL)
        return ENOENT;
    // Jeśli source i target są tym samym oraz source istnieje,
    // to kończymy przenoszenie bez robienia niczego.
    if (strcmp(source, target) == 0)
        return 0;

    Tree *next_component_target = tree;
    char component_target[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char folder_to_move_to[MAX_FOLDER_NAME_LENGTH_UTILS + 1];
    char *path_to_parent_target = make_path_to_parent(target, folder_to_move_to);
    const char *subpath_target = path_to_parent_target;
    while ((subpath_target = split_path(subpath_target, component_target))) {
        next_component_target =
                hmap_get(next_component_target->subfolders, component_target);
        if (next_component_target == NULL) {
            free(path_to_parent_target);
            return ENOENT;
        }
    }

    free(path_to_parent_target);

    if (hmap_get(next_component_target->subfolders, folder_to_move_to) != NULL)
        return EEXIST;

    Tree *new_node = malloc(sizeof(Tree));\
    if (new_node == NULL)
        exit(1);
    new_node->folder = strdup(target); // czy potrzebujemy całej ścieżki???
    new_node->subfolders = node_to_move->subfolders;
    hmap_remove(next_component_source->subfolders, folder_to_move);
    free(node_to_move->folder);
    free(node_to_move);
    hmap_insert(next_component_target->subfolders, folder_to_move_to, new_node);

    return 0;

}
