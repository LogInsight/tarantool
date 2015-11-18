/*
 * Copyright 2010-2015, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "box.h"
#include "cluster.h"
#include "recovery.h"
#include "applier.h"
#include <fiber.h>
#include <small/mempool.h>

/**
 * Globally unique identifier of this cluster.
 * A cluster is a set of connected appliers.
 */
tt_uuid cluster_id;

typedef rb_tree(struct server) serverset_t;
rb_proto(, serverset_, serverset_t, struct server)

static int
server_compare_by_uuid(const struct server *a, const struct server *b)
{
	return memcmp(&a->uuid, &b->uuid, sizeof(a->uuid));
}

rb_gen(, serverset_, serverset_t, struct server, link,
       server_compare_by_uuid);

static struct mempool server_pool;
static serverset_t serverset;

void
cluster_init(void)
{
	mempool_create(&server_pool, &cord()->slabc,
		       sizeof(struct server));
	serverset_new(&serverset);
}

void
cluster_free(void)
{
	mempool_destroy(&server_pool);
}

extern "C" struct vclock *
cluster_clock()
{
        return &recovery->vclock;
}

static struct server *
server_new(const struct tt_uuid *uuid)
{
	struct server *server = (struct server *) mempool_alloc(&server_pool);
	if (server == NULL) {
		diag_set(OutOfMemory, sizeof(*server), "malloc",
			 "struct server");
		return NULL;
	}
	server->id = 0;
	server->uuid = *uuid;
	server->applier = NULL;
	server->relay = NULL;
	return server;
}

static void
server_delete(struct server *server)
{
	assert(cserver_id_is_reserved(server->id));
	assert(server->applier == NULL);
	assert(server->relay == NULL);
	mempool_free(&server_pool, server);
}

/* Delete servers which doesn't have registered id, relay and applier */
static void
serverset_gc(serverset_t *set)
{
	struct server *server = serverset_first(set);
	while (server != NULL) {
		struct server *next = serverset_next(set, server);
		if (cserver_id_is_reserved(server->id) &&
		    server->applier == NULL && server->relay == NULL) {
			serverset_remove(set, server);
			server_delete(server);
		}
		server = next;
	}
}

struct server *
cluster_register_id(uint32_t server_id, const struct tt_uuid *server_uuid)
{
	struct recovery *r = ::recovery;
	/** Checked in the before-commit trigger */
	assert(!tt_uuid_is_nil(server_uuid));
	assert(!cserver_id_is_reserved(server_id) && server_id < VCLOCK_MAX);
	assert(!vclock_has(&r->vclock, server_id));

	struct server *server = server_by_uuid(server_uuid);
	if (server == NULL) {
		server = server_new(server_uuid);
		if (server == NULL)
			return NULL;
		server->id = server_id;
		serverset_insert(&serverset, server);
	} else {
		/* Checked by indexes in _cluster */
		assert(cserver_id_is_reserved(server->id));
		server->id = server_id;
	}

	/* Add server */
	vclock_add_server_nothrow(&r->vclock, server_id);
	if (tt_uuid_is_equal(&r->server_uuid, server_uuid)) {
		/* Assign local server id */
		assert(r->server_id == 0);
		r->server_id = server_id;
		/*
		 * Leave read-only mode
		 * if this is a running server.
		 * Otherwise, read only is switched
		 * off after recovery_finalize().
		 */
		if (r->writer)
			box_set_ro(false);
	}

	return server;
}

void
cluster_unregister_id(struct server *server)
{
	struct recovery *r = ::recovery;
	/** Checked in the before-commit trigger */
	assert(!cserver_id_is_reserved(server->id));

	vclock_del_server(&r->vclock, server->id);
	if (r->server_id == server->id) {
		r->server_id = 0;
		box_set_ro(true);
	}
	server->id = 0;
	serverset_gc(&serverset);
}

int
cluster_register_appliers(struct applier **appliers, int count)
{
	serverset_t uniq;
	serverset_new(&uniq);
	struct server *server;

	/* Check for duplicate UUID */
	for (int i = 0; i < count; i++) {
		server = server_new(&appliers[i]->uuid);
		if (server == NULL)
			goto error;

		if (serverset_search(&uniq, server) != NULL) {
			tnt_error(ClientError, ER_CFG, "replication_source",
				  "duplicate connection to the same server");
			goto error;
		}

		server->applier = appliers[i];
		serverset_insert(&uniq, server);
	}

	/*
	 * All invariants and conditions are checked, now it is safe to
	 * apply the new configuration. Nothing can fail after this point.
	 */

	/* Prube old appliers */
	for (server = serverset_first(&serverset);
	     server != NULL;
	     server = serverset_next(&serverset, server)) {
		if (server->applier == NULL)
			continue;
		applier_stop(server->applier); /* cancels a background fiber */
		applier_delete(server->applier);
		server->applier = NULL;
	}

	/* Save new appliers */
	server = serverset_first(&uniq);
	while (server != NULL) {
		struct server *next = serverset_next(&uniq, server);
		serverset_remove(&uniq, server);

		struct server *orig = serverset_search(&serverset, server);
		if (orig != NULL) {
			/* Use existing struct server */
			orig->applier = server->applier;
			assert(tt_uuid_is_equal(&orig->uuid,
						&orig->applier->uuid));
			server->applier = NULL;
			server_delete(server); /* remove temporary object */
			server = orig;
		} else {
			/* Add a new struct server */
			serverset_insert(&serverset, server);
		}

		say_warn("server: %s %p", tt_uuid_str(&server->uuid),
				server->applier);
		server = next;
	}

	assert(serverset_first(&uniq) == NULL);
	serverset_gc(&serverset);

	return 0;

error:
	server = serverset_first(&uniq);
	while (server != NULL) {
	     struct server *next = serverset_next(&uniq, server);
	     serverset_remove(&uniq, server);
	     server_delete(server);
	     server = next;
	}
	return -1;
}

void
cluster_register_relay(struct server *server, struct relay *relay)
{
	assert(!cserver_id_is_reserved(server->id));
	assert(server->relay == NULL);
	server->relay = relay;
}

void
cluster_unregister_relay(struct server *server)
{
	assert(server->relay != NULL);
	server->relay = NULL;
	serverset_gc(&serverset);
}

struct server *
server_first(void)
{
	return serverset_first(&serverset);
}

struct server *
server_next(struct server *server)
{
	return serverset_next(&serverset, server);
}

struct server *
server_by_uuid(const struct tt_uuid *uuid)
{
	struct server key;
	key.uuid = *uuid;
	return serverset_search(&serverset, &key);
}
