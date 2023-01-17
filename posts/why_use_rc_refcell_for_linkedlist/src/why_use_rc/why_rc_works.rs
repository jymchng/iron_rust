#![allow(unused)]
use std::rc::Rc;

#[derive(Clone)]
pub struct Node<T: Clone> {
    data: T,
    next: Option<Rc<Node<T>>>,
}

type OptionRcNode<T> = Option<Rc<Node<T>>>;

pub struct LinkedList<T: Clone> {
    head: OptionRcNode<T>,
    tail: OptionRcNode<T>,
}

impl<T: Clone> LinkedList<T> {
    pub fn new_empty() -> Self {
        Self {
            head: None,
            tail: None,
        }
    }

  pub fn new_with_node(node: Node<T>) -> Self {
    let mut new_ll = Self::new_empty();
    Self::append(&mut new_ll, node);
    new_ll
  }

  pub fn append(&mut self, node: Node<T>) {
    let appending_node = Rc::new(node);
    match self.tail.take() {
      Some(ref mut old_tail) => {
        let old_tail = Rc::make_mut(old_tail);
        old_tail.next = Some(Rc::clone(&appending_node));
      }
      None => {
        self.head = Some(Rc::clone(&appending_node));
      }
    }
    self.tail = Some(appending_node);
  }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_ll_new_empty() {
        let ll = LinkedList::<u32>::new_empty();
        assert!(ll.head.is_none() && ll.tail.is_none());
    }

  #[test]
  fn test_ll_new_with_node() {
    let new_node = Node {
      data: 16,
      next: None,
    };
    let ll = LinkedList::new_with_node(new_node);
    assert_eq!(ll.head.unwrap().data, 16);
    assert_eq!(ll.tail.unwrap().data, 16);
  }

  #[test]
  fn test_ll_append() {
    let new_node = Node {
      data: 16,
      next: None,
    };
    let mut ll = LinkedList::new_with_node(new_node);
    ll.append( Node {data: 17, next: None} );
    assert_eq!(ll.head.unwrap().next.as_ref().unwrap().data, 17);
  }
}

// Finished test [unoptimized + debuginfo] target(s) in 0.01s
// Running unittests src\lib.rs (target\debug\deps\why_use_rc_refcell_for_linkedlist-787cc8ba0a0d610b.exe)

// running 3 tests
// test why_use_rc::why_rc_works::tests::test_ll_new_empty ... ok
// test why_use_rc::why_rc_works::tests::test_ll_new_with_node ... ok
// test why_use_rc::why_rc_works::tests::test_ll_append ... FAILED

// failures:

// ---- why_use_rc::why_rc_works::tests::test_ll_append stdout ----
// thread 'why_use_rc::why_rc_works::tests::test_ll_append' panicked at 'called `Option::unwrap()` on a `None` value', src\why_use_rc\why_rc_works.rs:76:47 
// note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


// failures:
// why_use_rc::why_rc_works::tests::test_ll_append

// test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

// error: test failed, to rerun pass `--lib`