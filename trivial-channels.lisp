(in-package :trivial-channels.queue)

 ;; Stupid simple queue, no locking

(declaim (inline make-queue))
(defstruct queue head tail)

(defun queue-add-cons (q cons)
  (when cons
    (if (queue-tail q)
        (progn
          (rplacd (queue-tail q) cons)
          (setf (queue-tail q) (cdr (queue-tail q))))
        (progn
          (setf (queue-head q) cons)
          (setf (queue-tail q) cons)))
    cons))

(defun queue-add (q item)
  (car (queue-add-cons q (cons item nil))))

(defun queue-push (q item)
  (setf (queue-head q)
        (cons item (queue-head q)))
  (unless (queue-tail q)
    (setf (queue-tail q) (queue-head q)))
  item)

(defun queue-pop-cons (q)
  (let ((cons (queue-head q)))
    (when cons
      (setf (queue-head q) (cdr cons))
      (rplacd cons nil)
      (unless (queue-head q)
        (setf (queue-tail q) nil))
      cons)))

(defun queue-pop (q)
  (car (queue-pop-cons q)))

(defun queue-has-item-p (q)
  (not (null (queue-head q))))

(defun queue-peek (q)
  "Peek at the head of the queue."
  (car (queue-head q)))

(defun queue-pop-to (q1 q2)
  "Pop from `Q1`, adding to `Q2`, without consing."
  (let ((cons (queue-pop-cons q1)))
    (queue-add-cons q2 cons)
    (car cons)))

(defun queue-prepend-to (q1 q2)
  "Prepend all items in `Q1` to `Q2`, removing them from `Q1`"
  (unless (or (null (queue-head q1)))
    (rplacd (queue-tail q1) (queue-head q2))

    (when (null (queue-tail q2))
      (setf (queue-tail q2) (queue-tail q1)))

    (setf (queue-head q2) (queue-head q1))
    (setf (queue-head q1) nil)
    (setf (queue-tail q1) nil))
  (values))

 ;; Channels

(in-package :trivial-channels)

 ;; trivial channels

(defstruct channel
  (queue (make-queue) :type queue)
  (q-condition (bt:make-condition-variable))
  (q-mutex (bt:make-lock)))

(defun hasmsg (channel)
  "Return T if a message is available at the time of the call.  
Note: there is no guarantee that a subsequent call to getmsg will
  succeed if more than one thread is receiving from this channel."
  (bt:with-lock-held ((channel-q-mutex channel))
    (queue-has-item-p (channel-queue channel))))

(defun sendmsg (channel msg)
  "Send MSG to CHANNEL, notifying a thread that may be waiting."
  (bt:with-lock-held ((channel-q-mutex channel))
    (queue-add (channel-queue channel) msg)
    (bt:condition-notify (channel-q-condition channel))))

(defun %recvmsg (channel timeout)
  "The timeout version of recvmsg"
  (let ((abs-time (+ (get-internal-real-time)
		     (* internal-time-units-per-second
			timeout))))
    (bt:with-lock-held ((channel-q-mutex channel))
      (do ((has-item? (queue-has-item-p (channel-queue channel))
		      (queue-has-item-p (channel-queue channel))))
	  (has-item?
	   (values (queue-pop (channel-queue channel)) t))
	;; Exit when the condition-wait times out, or enough failed
	;; iterations through this loop result in no more time left.
	(unless (or (minusp timeout)
		    (bt:condition-wait (channel-q-condition channel)
				       (channel-q-mutex channel)
				       :timeout timeout))
	  (return (values nil nil)))
	;; Update the time left.  An expired timeout will be caught in
	;; the subsequent condition prior to calling condition-wait again.
	(decf timeout (/ (- abs-time
			    (get-internal-real-time))
			 internal-time-units-per-second))))))

(defun recvmsg (channel &optional timeout)
  "Get a message from CHANNEL.  When unavailable immediately, wait
TIMEOUT seconds (indefinitely when NULL) for a message to become
available.

When a message is returned, return (values message t)
When the recv times out,    return (values nil nil)"
  (if (numberp timeout)
      (%recvmsg channel timeout)
      (bt:with-lock-held ((channel-q-mutex channel))
	(do ((has-item? (queue-has-item-p (channel-queue channel))
			(queue-has-item-p (channel-queue channel))))
	    (has-item?
	     (values (queue-pop (channel-queue channel)) t))
	  (unless (bt:condition-wait (channel-q-condition channel)
				     (channel-q-mutex channel))
	    (return (values nil nil)))))))

(defun getmsg (channel)
  "Get a message from CHANNEL, returning (values message t) when
successful, and (values nil nil) when no message is available."
  (bt:with-lock-held ((channel-q-mutex channel))
    (if (queue-has-item-p (channel-queue channel))
	(values (queue-pop (channel-queue channel)) t)
	(values nil nil))))
