//! An incremental implementation of worst-case optimal joins.
//!
//! This crate contains functionality to construct timely dataflow computations to compute and maintain 
//! the results of complex relational joins under changes to the relations, with worst-case optimality 
//! guarantees about the running time.
//! 
//! As an example, consider a stream of directed graph edges `(src, dst)` where we would like to find all 
//! directed cycles of length three. That is, node indentifiers `x0`, `x1`, and `x2` where the graph contains
//! edges `(x0, x1)`, `(x1, x2)`, and `(x2, x0)`. We can write this query as a relational join on the edge
//! relation `edge(x,y)`, as
//!
//! cycle_3(x0, x1, x2) := edge(x0, x1), edge(x1, x2), edge(x2, x0)
//!
//! To determine the set of three-cycles, we could use standard techniques from the database literature to 
//! perform the join, typically first picking one attribute (`x0`, `x1`, or `x2`) and performing the join on
//! the two relations containing that attribute, then joining (intersecting) with the remaining relation.
//! 
//! This has the defect that it may perform an amount of work quadratic in the size of `edges`. Recent work 
//! on "worst-case optimal join processing" shows how to get around this problem, by considering multiple 
//! relations at the same time.
//!
//! This crate is a streaming implementation of incremental worst-case optimal join processing. You may 
//! indicate a relational query like above, and the crate with synthesize a timely dataflow computation which
//! reports all changes to the occurrences of satisfying assignments to the values. The amount of work performed
//! is no more than the worst-case optimal bound.

extern crate timely;

pub mod timely_rule;
pub mod wings_rule;