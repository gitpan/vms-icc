/*
 *
 * ICC.xs
 *
 * The XS interface to the ICC services 
 *
 * Modification History
 *
 * 07/26/99	DRS	Created, more or less
 */
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include <iccdef.h>
#include <libdef.h>
#include <builtins.h>
#include <ssdef.h>
#include <descrip.h>
#include <starlet.h>
#include <lib$routines.h>
#include <iosbdef.h>

/* The biggest message we're willing to receive */
#define MAX_MESSAGE_SIZE 1000

static long max_entries = 25;	/* Maximum number of entries we'll	*/
				/* handle at one time */
static long queue_head[2];	/* Queue header */
static long queue_count;	/* Number of entries in the queue */
static long accepting_connections = 0;	/* True if we're accepting	*/
					/* connections, otherwise false */
static long ServiceInUse = 0;	/* Have we actually started? */

struct queue_entry {
	char private_to_queue[8];
	unsigned int conn_handle;
};

/* An AST routine to accept incoming connections and put them on a */
/* queue */
void connect_call(unsigned int event_type, unsigned int conn_handle,
		  unsigned int data, char *data_bfr, unsigned int P5,
		  unsigned int P6, char *P7)
{
  struct queue_entry *queue_entry;
  int status;
  
  /* Are we accepting any new connections? */
  if ((queue_count > max_entries) || !accepting_connections) {
    /* Nope. Tell 'em to go away */
    sys$icc_reject(conn_handle);
  } else {
    /* Yep. Increment the connect count and aquire the		*/
    /* connection */
    status = sys$icc_accept(conn_handle, NULL, 0, 0, 0);
    if (status == SS$_NORMAL) {
      /* Take the accepted connection, make a note of it, build a */
      /* queue entry, and put it on the queue */

      /* Gotta be atomic, otherwise it's not threadsafe. (Or AST safe, */
      /* for that matter) */
      __ATOMIC_INCREMENT_LONG(&queue_count);
      queue_entry = malloc(sizeof(queue_entry));
      queue_entry->conn_handle = conn_handle;
      lib$insqti(queue_entry, queue_head);
    }
  }
  return;
}

char * ss_translate(int status)
{
  switch(status) {
  case SS$_NORMAL: return "SS$_NORMAL";
  case SS$_ACCVIO: return "SS$_ACCVIO";
  case SS$_BADPARAM: return "SS$_BADPARAM";
  case SS$_DUPLNAM: return "SS$_DUPLNAM";
  case SS$_EXQUOTA: return "SS$_EXQUOTA";
  case SS$_INSFARG: return "SS$_INSFARG";
  case SS$_INSFMEM: return "SS$_INSFMEM";
  case SS$_IVMODE: return "SS$_IVMODE";
  case SS$_NOLINKS: return "SS$_NOLINKS";
  case SS$_NONETMBX: return "SS$_NONETMBX";
  case SS$_NOPRIV: return "SS$_NOPRIV";
  case SS$_SSFAIL: return "SS$_SSFAIL";
  case SS$_TOO_MANY_ARGS: return "SS$_TOO_MANY_ARGS";
  default: return "dunno";
  }
}

MODULE = VMS::ICC		PACKAGE = VMS::ICC		

BOOT:
queue_count = 0;
Zero(queue_head, 2, long);


SV *
new_service(service_name = &PL_sv_undef, logical_name = &PL_sv_undef, logical_table = &PL_sv_undef)
     SV *service_name
     SV *logical_name
     SV *logical_table

   PPCODE:
{

  struct dsc$descriptor LogicalTable;
  struct dsc$descriptor LogicalName;
  struct dsc$descriptor ServiceName;

  $DESCRIPTOR(DefaultLogTable, "ICC$REGUSTRY_TABLE");

  struct dsc$descriptor *LogicalTablePtr, *LogicalNamePtr, *ServiceNamePtr;

  unsigned int AssocHandle, status;

  /* Three parameters. service_name can be undef, in which case we'll   
     use the default service name. If logical_table is undef, then we'll   
     use the default ICC logical table */

  /* First, is the service already set up? die if it is */
  if (ServiceInUse) {
    croak("Service already registered");
    XSRETURN_UNDEF;
  }


  /* If the name's undef, use the default name, otherwise fill in the */
  /* blanks appropriately */
  if (!SvOK(service_name)) {
    ServiceNamePtr = NULL;
  } else {
    ServiceName.dsc$b_dtype = DSC$K_DTYPE_T;
    ServiceName.dsc$b_class = DSC$K_CLASS_S;
    ServiceName.dsc$a_pointer = SvPV(service_name, PL_na);
    ServiceName.dsc$w_length = SvCUR(service_name);
    ServiceNamePtr = &ServiceName;
  }
    
  /* If the name's undef, use the default name, otherwise fill in the */
  /* blanks appropriately */
  if (!SvOK(logical_name)) {
    LogicalNamePtr = NULL;
  } else {
    LogicalName.dsc$b_dtype = DSC$K_DTYPE_T;
    LogicalName.dsc$b_class = DSC$K_CLASS_S;
    LogicalName.dsc$a_pointer = SvPV(logical_name, PL_na);
    LogicalName.dsc$w_length = SvCUR(logical_name);
    LogicalNamePtr = &LogicalName;
  }
    
  /* If the name's undef, use the default name, otherwise fill in the */
  /* blanks appropriately */
  if (!SvOK(logical_table)) {
    if (SvOK(logical_name)) {
      LogicalTablePtr = (struct dsc$descriptor *)&DefaultLogTable;
    } else {
      LogicalTablePtr = NULL;
    }
  } else {
    LogicalTable.dsc$b_dtype = DSC$K_DTYPE_T;
    LogicalTable.dsc$b_class = DSC$K_CLASS_S;
    LogicalTable.dsc$a_pointer = SvPV(logical_table, PL_na);
    LogicalTable.dsc$w_length = SvCUR(logical_table);
    LogicalTablePtr = &LogicalTable;
  }
    

  status = sys$icc_open_assoc(&AssocHandle, ServiceNamePtr, LogicalNamePtr,
			      LogicalTablePtr, connect_call, NULL, NULL,
			      0, 0);

  /* Did it work? */
  if ($VMS_STATUS_SUCCESS(status)) {
    accepting_connections = 1;
    ServiceInUse = 1;
    XPUSHs(sv_2mortal(newSViv(AssocHandle)));
    XSRETURN(1);
  } else {
    printf("Error %s\n", ss_translate(status));
    SETERRNO(EVMSERR, status);
    XSRETURN_UNDEF;
  }
}

SV *
accept_connection(service_handle = &PL_sv_undef)
     SV *service_handle
   PPCODE:
{
/* Accept an outstanding connection. (Well, one that we've already */
/* officially accepted, just not acknowledged) The parameter's */
/* currently ignored as we only allow one connection listener at the */
/* moment, but that could change in the future */

  SV *return_sv;
  struct queue_entry *queue_entry;
  /* try and take an entry off the queue and see what happens */
  lib$remqhi(queue_head, &queue_entry);
  /* If we got back the address of the queue_head, then there wasn't */
  /* anything to be had and we can exit with an undef */
  /* Yeah, I know casting these both to longs is skanky. But it shuts */
  /* the compiler up */
  if ((long)queue_entry == (long)queue_head) {
    XSRETURN_UNDEF;
  }

  /* Decrement the queue count */
  __ATOMIC_DECREMENT_LONG(&queue_count);

  /* Hey, look--we got something. Pass it back. This counts on the */
  /* fact that a pointer will fit into the IV slot of an SV. This is */
  /* probably a bad, bad thing I'm doing here... */
  XPUSHs(sv_2mortal(newSViv(queue_entry->conn_handle)));

  /* Free up the queue entry */
  Safefree(queue_entry);

  XSRETURN(1);
}

SV *
read_data(connection_handle)
     SV *connection_handle
   PPCODE:
{
  SV *received_data;
  int status;
  ios_icc ICC_Info;

  /* Create us a new SV with MAX_MESSAGE_SIZE bytes allocated. Mark it */
  /* as mortal, too */
  received_data = NEWSV(912, MAX_MESSAGE_SIZE);
  sv_2mortal(received_data);

  /* Go look for some data */
  status = sys$icc_receivew(SvIV(connection_handle), &ICC_Info, NULL, 0,
			    SvPVX(received_data), MAX_MESSAGE_SIZE);
  /* Did it go OK? */
  if (SS$_NORMAL == status) {
    /* Set the scalar length */
    SvCUR(received_data) = ICC_Info.ios_icc$l_rcv_len;
    SvPOK_on(received_data);
    XPUSHs(received_data);
    XSRETURN(1);
  } else {
    /* Guess something went wrong. Return the status */
    SETERRNO(EVMSERR, status);
    XSRETURN_UNDEF;
  }
}

SV *
write_data(connection_handle, data, async = &PL_sv_undef)
     SV *connection_handle
     SV *data
     SV *async
   CODE:
{
  ios_icc ICC_Info;
  int status;
  /* Just return with ACCVIO they gave us no data. That's what'd */
  /* happen, after all, if we tried passing a null buffer around */
  if (!SvCUR(data)) {
    SETERRNO(EVMSERR, SS$_ACCVIO);
    XSRETURN_UNDEF;
  }

  if (SvTRUE(async)) {
    status = sys$icc_transmit(SvIV(connection_handle), &ICC_Info, NULL, NULL,
			      SvPVX(data), SvCUR(data));
  } else {
    status = sys$icc_transmitw(SvIV(connection_handle), &ICC_Info, NULL, NULL,
			       SvPVX(data), SvCUR(data));
  }
  if (SS$_NORMAL == status) {
    XSRETURN_YES;
  } else {
    SETERRNO(EVMSERR, status);
    XSRETURN_UNDEF;
  }
}

SV *
close_connection(connection_handle)
     SV *connection_handle
   CODE:
{
  int status;
  iosb iosb;
  status = sys$icc_disconnectw(SvIV(connection_handle), &iosb,0,0,0,0);
  if (SS$_NORMAL == status) {
    XSRETURN_YES;
  } else {
    SETERRNO(EVMSERR, status);
    XSRETURN_UNDEF;
  }
}

SV *
delete_service(service_handle=&PL_sv_undef)
     SV *service_handle
   CODE:
{
  struct queue_entry *queue_entry;
  iosb iosb;

  /* Close the association */
  sys$icc_close_assoc(SvIV(service_handle));

  /* Note that we're not accepting any more */
  accepting_connections = 0;

  /* Run through the connections we've got and disconnect them */
  lib$remqhi(queue_head, &queue_entry);
  while (queue_head != (long *)queue_entry) {
    sys$icc_disconnectw(queue_entry->conn_handle, &iosb);
    Safefree(queue_entry);
  }

  /* Note that the queue is empty */
  queue_count = 0;

  /* Mark the service as not in use */
  ServiceInUse = 0;

  /* return OK */
  XSRETURN_YES;
}

SV *
open_connection(assoc_name, node = &PL_sv_undef)
     SV *assoc_name
     SV *node
   PPCODE:
{
  struct dsc$descriptor_s AssocName, NodeName;
  struct dsc$descriptor_s *AssocNamePtr, *NodeNamePtr;
  int status;
  
  unsigned int ConnHandle;

  ios_icc IOS_ICC;

  /* 'Kay, validate our stuff */
  if (!SvOK(assoc_name)) {
    croak("association name may not be undef");
  }
    
  /* If the name's undef, use the default name, otherwise fill in the */
  /* blanks appropriately */
  if (!SvOK(assoc_name)) {
    AssocNamePtr = NULL;
  } else {
    AssocName.dsc$b_dtype = DSC$K_DTYPE_T;
    AssocName.dsc$b_class = DSC$K_CLASS_S;
    AssocName.dsc$a_pointer = SvPV(assoc_name, PL_na);
    AssocName.dsc$w_length = SvCUR(assoc_name);
    AssocNamePtr = &AssocName;
  }
    
  /* If the name's undef, use the default name, otherwise fill in the */
  /* blanks appropriately */
  if (!SvOK(node)) {
    NodeNamePtr = NULL;
  } else {
    NodeName.dsc$b_dtype = DSC$K_DTYPE_T;
    NodeName.dsc$b_class = DSC$K_CLASS_S;
    NodeName.dsc$a_pointer = SvPV(node, PL_na);
    NodeName.dsc$w_length = SvCUR(node);
    NodeNamePtr = &NodeName;
  }

  status = sys$icc_connect(&IOS_ICC, NULL, NULL,
			   ICC$C_DFLT_ASSOC_HANDLE, &ConnHandle,
			   AssocNamePtr, NodeNamePtr, 0, NULL, 0,
			   NULL, NULL, NULL, 0);
  if (SS$_NORMAL == status) {
    /* Hey, look--we got something. Pass it back. This counts on the */
    /* fact that a pointer will fit into the IV slot of an SV. This is */
    /* probably a bad, bad thing I'm doing here... */
    XPUSHs(sv_2mortal(newSViv(ConnHandle)));
    XSRETURN(1);
  } else {
    printf("Error %s\n", ss_translate(status));
    SETERRNO(EVMSERR, status);
    XSRETURN_UNDEF;
  }
}

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            