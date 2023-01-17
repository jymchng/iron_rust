#![allow(unused)]

pub struct Node<T> {
    data: T,
    next: Option<Box<Node<T>>>,
}

type OptionBoxedNode<T> = Option<Box<Node<T>>>;

pub struct LinkedList<T> {
    head: OptionBoxedNode<T>,
    tail: OptionBoxedNode<T>,
}

impl<T> LinkedList<T> {
    pub fn new_empty() -> Self {
        Self {
            head: None,
            tail: None,
        }
    }

  pub fn new_with_node(node: Node<T>) -> Self {
    Self {
      head: Some(Box::new(node)),
      tail: None,
    }
  }

  pub fn append(&mut self, node: Node<T>) {
    match self.head.take() {
      Some(mut old_head) => {
        old_head.next = Some(Box::new(node));
        self.tail = Some(Box::new(node));
      }
      None => {
        self.head = Some(Box::new(node));
      }
    }
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
    assert_eq!(ll.head.unwrap().data, 16)
  }

  #[test]
  fn test_ll_append() {
    let new_node = Node {
      data: 16,
      next: None,
    };
    let mut ll = LinkedList::new_with_node(new_node);
    ll.append( Node {data: 17, next: None} );
  }
}

// error[E0382]: use of moved value: `node`
//   --> src/why_use_rc_refcell/why_box_not_works.rs:34:35
//    |
// 30 |   pub fn append(&mut self, node: Node<T>) {
//    |                            ---- move occurs because `node` has type `Node<T>`, which does not implement the `Copy` trait
// ...
// 33 |         old_head.next = Some(Box::new(node));
//    |                                       ---- value moved here
// 34 |         self.tail = Some(Box::new(node));
//    |                                   ^^^^ value used here after move