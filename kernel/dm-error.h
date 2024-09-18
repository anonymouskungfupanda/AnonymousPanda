#include <linux/blk_types.h>

#define BLK_STS_OFFLINE		((__force blk_status_t)16)

static const struct {
	int		errno;
	const char	*name;
} blk_errors[] = {
	[BLK_STS_OK]		    = { 0,		"" },
	[BLK_STS_NOTSUPP]	    = { -EOPNOTSUPP,"operation not supported" },
	[BLK_STS_TIMEOUT]	    = { -ETIMEDOUT,	"timeout" },
	[BLK_STS_NOSPC]		    = { -ENOSPC,	"critical space allocation" },
	[BLK_STS_TRANSPORT]	    = { -ENOLINK,	"recoverable transport" },
	[BLK_STS_TARGET]	    = { -EREMOTEIO,	"critical target" },
	[BLK_STS_MEDIUM]	    = { -ENODATA,	"critical medium" },
	[BLK_STS_PROTECTION]	= { -EILSEQ,	"protection" },
	[BLK_STS_RESOURCE]	    = { -ENOMEM,	"kernel resource" },
	[BLK_STS_DEV_RESOURCE]	= { -EBUSY,	    "device resource" },
	[BLK_STS_AGAIN]		    = { -EAGAIN,	"nonblocking retry" },
	[BLK_STS_OFFLINE]	    = { -ENODEV,	"device offline" },

	/* device mapper special case, should not leak out: */
	[BLK_STS_DM_REQUEUE]	= { -EREMCHG, "dm internal retry" },

	/* zone device specific errors */
	[BLK_STS_ZONE_OPEN_RESOURCE]	= { -ETOOMANYREFS, "open zones exceeded" },
	[BLK_STS_ZONE_ACTIVE_RESOURCE]	= { -EOVERFLOW, "active zones exceeded" },

	/* everything else not covered above: */
	[BLK_STS_IOERR]		= { -EIO,	"I/O" },
};

blk_status_t errno_to_blk_status(int errno)
{
	int i;

	for (i = 0; i < ARRAY_SIZE(blk_errors); i++) {
		if (blk_errors[i].errno == errno)
			return (__force blk_status_t)i;
	}

	return BLK_STS_IOERR;
}

int blk_status_to_errno(blk_status_t status)
{
	int idx = (__force int)status;

	if (WARN_ON_ONCE(idx >= ARRAY_SIZE(blk_errors)))
		return -EIO;
	return blk_errors[idx].errno;
}

const char *blk_status_to_str(blk_status_t status)
{
	int idx = (__force int)status;

	if (WARN_ON_ONCE(idx >= ARRAY_SIZE(blk_errors)))
		return "<null>";
	return blk_errors[idx].name;
}

