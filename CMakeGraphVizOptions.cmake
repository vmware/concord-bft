# Use "--graphviz=concord-bft.dot" cmake command option for dependency graph generation.
# Use "dot -Tpng -o concord-bft.png concord-bft.dot" to generate png image of the gpaph.

set(GRAPHVIZ_GRAPH_NAME "concord-bft")
set(GRAPHVIZ_GENERATE_PER_TARGET FALSE)
set(GRAPHVIZ_EXECUTABLES FALSE)
set(GRAPHVIZ_SHARED_LIBS FALSE)
set(GRAPHVIZ_EXTERNAL_LIBS FALSE)
set(GRAPHVIZ_UNKNOWN_LIBS FALSE)
