

//TODO(mengke): These code is useless, but I want to keep it


template<typename F, typename T>
void call(GlobalAddress<T> t, F func){
  size_t from_node = node_rank();
  size_t from_thread = thread_rank();
  size_t from_id = from_node * THREAD_SIZE + from_thread;
  size_t to_id = t.get_id();

  /* short-cut */
  if( from_id == to_id ){
    func( t.ptr() );
  }else{
    call_inflight[from_thread]->issue();
    send_request(from_id, to_id, [from_id, t, func]{
      func( t.ptr() );
      /* send complete msg*/
      send_completion(from_id);
    });//send_request to
  }
}

template<typename F>
void call(Barrier* B, size_t id, F func){
  size_t from_node = node_rank();
  size_t from_thread = thread_rank();
  size_t to_node = id/thread_size();
  size_t to_thread = id%thread_size();

  /* short-cut */
  if(from_thread == to_thread && from_node == to_node){
    func();
  }else{
    send_request(from_node, from_thread, to_node, to_thread, 
        [B, from_node, from_thread, to_node, to_thread, func]{
  
      func();
      /* send complete msg*/
      send_request(to_node, to_thread, from_node, from_thread, 
          [B, from_thread]{
        B->complete();
      });//send_request back
    });//send_request to
  }
}


template<typename F>
void call(size_t id, F func){
  size_t from_node = node_rank();
  size_t from_thread = thread_rank();
  size_t from_id = from_node * THREAD_SIZE + from_thread;
  size_t to_id = id;

  /* short-cut */
  if(from_id == to_id){
    func();
  }else{
    call_inflight[from_thread]->issue();
    send_request(from_id, to_id, [from_id, func]{
      func();
      /* send complete msg*/
      send_completion(from_id);
    });//send_request to
  }
}


template<typename F, typename T>
auto rcall(GlobalAddress<T> t, F func) 
  -> std::unique_ptr<Future<decltype( func(t.ptr()) )>> {

  using ret_type=decltype( func(t.ptr()) );

  size_t from_node = node_rank();
  size_t from_thread = thread_rank();
  size_t to_node = t.get_node_rank();
  size_t to_thread = t.get_thread_rank();

  std::unique_ptr<Future<ret_type>> fu(new Future<ret_type>());
  auto pfu = fu.get();

  /* short-cut */
  if(from_thread == to_thread && from_node == to_node){
    auto ans = func(t.ptr());
    pfu->set_value(ans);
  }else{
    call_inflight[from_thread]->issue();
    send_request(from_node, from_thread, to_node, to_thread, 
        [from_node, from_thread, to_node, to_thread, t, func, pfu]{
  
      auto ans = func( t.ptr() );
  
      /* send complete msg*/
      send_request(to_node, to_thread, from_node, from_thread, 
          [ans, pfu, from_thread]{
        pfu->set_value(ans);
        call_inflight[from_thread]->complete();
      });
    });
  }
  return fu;
}

template<typename F>
auto rcall(size_t id, F func) 
  -> std::unique_ptr<Future<decltype( func() )>> {

  using ret_type=decltype( func() );

  size_t from_node = node_rank();
  size_t from_thread = thread_rank();
  size_t to_node = id/thread_size();
  size_t to_thread = id%thread_size();

  std::unique_ptr<Future<ret_type>> fu(new Future<ret_type>());
  auto pfu = fu.get();

  if(from_thread == to_thread && from_node == to_node){
      auto ans = func();
      pfu->set_value(ans);
  }else{
    call_inflight[from_thread]->issue();
    send_request(from_node, from_thread, to_node, to_thread, 
        [from_node, from_thread, to_node, to_thread, func, pfu]{

      auto ans = func();

      /* send complete msg*/
      send_request(to_node, to_thread, from_node, from_thread, 
          [ans, pfu, from_thread]{
        pfu->set_value(ans);
        call_inflight[from_thread]->complete();
      });
    });
  }
  return fu;
}




/* call with data */
template<typename F, typename T, typename R>
void call_with_data(GlobalAddress<T> t, std::vector<R> data, F func){
  size_t from_node = node_rank();
  size_t from_thread = thread_rank();
  size_t to_node = t.get_node_rank();
  size_t to_thread = t.get_thread_rank();

  call_inflight[from_thread]->issue();

  send_request_with_payload(from_node, from_thread, to_node, to_thread, data,
      [from_node, from_thread, to_node, to_thread, t, func]{

    func( t.ptr() );

    /* send complete msg*/
    send_request(to_node, to_thread, from_node, from_thread, 
        [from_thread]{
      call_inflight[from_thread]->complete();
    });//send_request back

  });//send_request to
}

template<typename F, typename R>
void call_with_data(size_t id, std::vector<R> data, F func){
  size_t from_node = node_rank();
  size_t from_thread = thread_rank();
  size_t to_node = id/thread_size();
  size_t to_thread = id%thread_size();

  call_inflight[from_thread]->issue();

  send_request_with_payload(from_node, from_thread, to_node, to_thread, data,
      [from_node, from_thread, to_node, to_thread, func]{

    func();

    /* send complete msg*/
    send_request(to_node, to_thread, from_node, from_thread, 
        [from_thread]{
      call_inflight[from_thread]->complete();
    });//send_request back

  });//send_request to
}

template<typename F, typename T, typename R>
auto rcall_with_data(GlobalAddress<T> t, std::vector<R> data,  F func) 
  -> std::unique_ptr<Future<decltype( func(t.ptr()) )>> {

  using ret_type=decltype( func(t.ptr()) );

  size_t from_node = node_rank();
  size_t from_thread = thread_rank();
  size_t to_node = t.get_node_rank();
  size_t to_thread = t.get_thread_rank();

  std::unique_ptr<Future<ret_type>> fu(new Future<ret_type>());
  auto pfu = fu.get();

  call_inflight[from_thread]->issue();

  send_request_with_payload(from_node, from_thread, to_node, to_thread, data,
      [from_node, from_thread, to_node, to_thread, t, func, pfu]{

    auto ans = func( t.ptr() );

    /* send complete msg*/
    send_request(to_node, to_thread, from_node, from_thread, 
        [ans, pfu, from_thread]{
      pfu->set_value(ans);
      call_inflight[from_thread]->complete();
    });

  });

  return fu;
}

template<typename F, typename R>
auto rcall(size_t id, std::vector<R> data, F func) 
  -> std::unique_ptr<Future<decltype( func() )>> {

  using ret_type=decltype( func() );

  size_t from_node = node_rank();
  size_t from_thread = thread_rank();
  size_t to_node = id/thread_size();
  size_t to_thread = id%thread_size();

  std::unique_ptr<Future<ret_type>> fu(new Future<ret_type>());
  auto pfu = fu.get();

  call_inflight[from_thread]->issue();

  send_request_with_payload( from_node, from_thread, to_node, to_thread, data,
      [from_node, from_thread, to_node, to_thread, func, pfu]{

    auto ans = func();

    /* send complete msg*/
    send_request(to_node, to_thread, from_node, from_thread, 
        [ans, pfu, from_thread]{
      pfu->set_value(ans);
      call_inflight[from_thread]->complete();
    });

  });

  return fu;
}

