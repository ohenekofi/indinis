// src/lsm/skiplist.cpp

#include "../../include/lsm/skiplist.h"
#include <cstring>
#include <cassert>

namespace engine {
namespace lsm {

// --- Internal Node Struct Implementation ---
struct SkipList::Node {
private:
    const char* key_ptr;
    size_t key_size;
    Value* value_ptr; // Pointer to the Value struct in the arena
    std::atomic<Node*> next_[1];

public:
    static Node* Create(Arena& arena, const Key& key, Value* value, int height) {
        const size_t key_size = key.size();
        size_t node_header_size = sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1);
        char* mem = arena.Allocate(node_header_size + key_size);
        
        Node* node = new (mem) Node();
        node->key_ptr = mem + node_header_size;
        node->key_size = key_size;
        node->value_ptr = value;
        std::memcpy((void*)node->key_ptr, key.data(), key_size);
        
        return node;
    }
    
    Value* GetValue() const { return value_ptr; }
    Key GetKey() const { return Key(key_ptr, key_size); }

    Node* Next(int n) { return next_[n].load(std::memory_order_acquire); }
    void SetNext(int n, Node* x) { next_[n].store(x, std::memory_order_release); }
    bool CompareAndSetNext(int n, Node* expected, Node* x) {
        return next_[n].compare_exchange_strong(expected, x);
    }
};

// --- SkipList Method Implementations ---

SkipList::SkipList(Arena& arena)
    : arena_(arena),
      head_(NewNode("", nullptr, kMaxHeight)),
      max_height_(1),
      count_(0),
      rnd_generator_(std::random_device{}()),
      rnd_dist_(0, 1) {
    for (int i = 0; i < kMaxHeight; ++i) {
        head_->SetNext(i, nullptr);
    }
}

void SkipList::Add(const std::string& key, const std::string& value, uint64_t seq) {
    Upsert(key, value, seq, false);
}

void SkipList::Delete(const std::string& key, uint64_t seq) {
    // For a tombstone, the value string is empty.
    Upsert(key, "", seq, true);
}

bool SkipList::Get(const std::string& key, std::string& value, uint64_t& seq) const {
    Node* x = FindGreaterOrEqual(key, nullptr);
    if (x != nullptr && x->GetKey() == key) {
        Value* found_value = x->GetValue();
        if (found_value && !found_value->is_tombstone) {
            value = found_value->value_str;
            seq = found_value->sequence;
            return true;
        }
    }
    return false;
}

std::unique_ptr<MemTableIterator> SkipList::NewIterator() const {
    return std::make_unique<Iterator>(this);
}

size_t SkipList::ApproximateMemoryUsage() const {
    return arena_.MemoryUsage();
}

size_t SkipList::Count() const {
    return count_.load(std::memory_order_relaxed);
}

bool SkipList::Empty() const {
    return Count() == 0;
}

// --- Private Helper Implementations ---

void SkipList::Upsert(const std::string& key, const std::string& value_str, uint64_t seq, bool is_tombstone) {
    Node* prev[kMaxHeight];
    
    int height = RandomHeight();
    if (height > max_height_.load(std::memory_order_relaxed)) {
        max_height_.store(height, std::memory_order_relaxed);
    }
    
    // Allocate and construct the new Value and Node in the arena
    void* value_mem = arena_.Allocate(sizeof(Value));
    Value* new_value = new (value_mem) Value{value_str, seq, is_tombstone};
    Node* x = NewNode(key, new_value, height);

    while (true) {
        FindGreaterOrEqual(key, prev);
        
        bool cas_failed = false;
        for (int i = 0; i < height; i++) {
            // Read the 'next' pointer that our predecessor is currently pointing to.
            Node* expected_next = prev[i]->Next(i);
            // Link our new node to point to that same 'next' node.
            x->SetNext(i, expected_next);
            
            // Atomically attempt to swing the predecessor's 'next' pointer from
            // its old value ('expected_next') to our new node ('x').
            if (!prev[i]->CompareAndSetNext(i, expected_next, x)) {
                // Another thread interfered and changed prev[i]->next_[i] before we could.
                // Our insertion on this level failed. We must restart the whole process.
                cas_failed = true;
                break; // Exit the for-loop
            }
        }
        
        if (cas_failed) {
            // A race condition was detected. Retry the entire operation.
            continue;
        }
        
        // If we reach here, all CAS operations for all levels succeeded.
        count_.fetch_add(1, std::memory_order_relaxed);
        return; // Success
    }
}

SkipList::Node* SkipList::NewNode(const Key& key, Value* value, int height) {
    return Node::Create(arena_, key, value, height);
}

SkipList::Node* SkipList::FindGreaterOrEqual(const Key& key, Node** prev) const {
    Node* x = head_;
    int level = max_height_.load(std::memory_order_relaxed) - 1;
    while (true) {
        Node* next = x->Next(level);
        if (KeyIsAfterNode(key, next)) {
            x = next;
        } else {
            if (prev != nullptr) prev[level] = x;
            if (level == 0) {
                return next;
            } else {
                level--;
            }
        }
    }
}
SkipList::Node* SkipList::FindLessThan(const Key& key) const {
    Node* x = head_;
    // Start search from the highest level of the list.
    int level = max_height_.load(std::memory_order_relaxed) - 1;
    
    while (true) {
        // Assert that the current node 'x' has a key less than the target key.
        // This is an invariant of the loop.
        assert(x == head_ || x->GetKey().compare(key) < 0);
        
        Node* next = x->Next(level);
        
        // If the next node is null or its key is >= the target key, we can't move
        // forward on this level. We must drop down to a lower level.
        if (next == nullptr || next->GetKey().compare(key) >= 0) {
            if (level == 0) {
                // We are at the bottom level. 'x' is the node we're looking for.
                return x;
            } else {
                // Move down to the next level and continue the search from 'x'.
                level--;
            }
        } else {
            // The next node's key is still less than the target. We can safely move forward.
            x = next;
        }
    }
}

SkipList::Node* SkipList::FindLast() const {
    Node* x = head_;
    // Start search from the highest level of the list.
    int level = max_height_.load(std::memory_order_relaxed) - 1;
    
    while (true) {
        Node* next = x->Next(level);
        if (next == nullptr) {
            // We've hit the end of the list at this level.
            if (level == 0) {
                // If we are at the bottom level and the next is null, then 'x' is the last node.
                // If 'x' is the head node itself, it means the list is empty.
                return (x == head_) ? nullptr : x;
            } else {
                // Move down to a lower level to continue the search for the absolute end.
                level--;
            }
        } else {
            // There is another node at this level, so we can move forward.
            x = next;
        }
    }
}

int SkipList::RandomHeight() {
    int height = 1;
    while (height < kMaxHeight && rnd_dist_(rnd_generator_) == 1) {
        height++;
    }
    return height;
}

bool SkipList::KeyIsAfterNode(const Key& key, Node* n) const {
    return (n != nullptr) && (key.compare(n->GetKey()) > 0);
}


// --- SkipList::Iterator Implementation ---
SkipList::Iterator::Iterator(const SkipList* list) : list_(list), node_(nullptr) {}
bool SkipList::Iterator::Valid() const { return node_ != nullptr; }
void SkipList::Iterator::Next() { assert(Valid()); node_ = node_->Next(0); }
void SkipList::Iterator::Prev() { assert(Valid()); node_ = list_->FindLessThan(node_->GetKey()); if (node_ == list_->head_) { node_ = nullptr; } }
void SkipList::Iterator::SeekToFirst() { node_ = list_->head_->Next(0); }
void SkipList::Iterator::SeekToLast() { node_ = list_->FindLast(); }
void SkipList::Iterator::Seek(const std::string& key) { node_ = list_->FindGreaterOrEqual(key, nullptr); }
Entry SkipList::Iterator::GetEntry() const {
    assert(Valid());
    Value* val = node_->GetValue();
    // For a tombstone, the value string in the Entry will be empty.
    return {std::string(node_->GetKey()), val->value_str, val->sequence};
}

} // namespace lsm
} // namespace engine